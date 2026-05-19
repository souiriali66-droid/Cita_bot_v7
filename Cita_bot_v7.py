#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🇪🇸 CITA PREVIA BOT — V7 ELITE
Fixes from V6 review:

✅ aiohttp replaces requests (fully non-blocking)
✅ Proxy intelligence engine (score, cooldown, rotation)
✅ Dedicated async DB writer (no write lock contention)
✅ Fixed circuit breaker (captchas/attempts — accurate ratio)
✅ Per-worker RateController + global penalty multiplier
✅ Cron-style scheduler (no queue.empty() race condition)
✅ Watchdog (memory + stuck task + event loop lag)
✅ Profile rotation on captcha storm (24h or on threshold)
✅ WhatsApp rate limited via aiohttp
✅ Detection: clickable+enabled booking flow
✅ Human behavior: bezier curves, reading pauses, mis-hover
"""

import os
import re
import json
import time
import random
import signal
import logging
import asyncio
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from urllib.parse import quote

import aiohttp

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Page, BrowserContext
)

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("V7_ELITE")

# =========================================================
# CONFIG
# =========================================================

@dataclass
class AppConfig:
    headless: bool = True
    max_workers: int = 1
    max_retries_per_step: int = 3
    workflow_timeout_ms: int = 60000
    alert_cooldown: int = 300

    # Scheduling
    check_interval_min: int = 20
    check_interval_max: int = 45
    peak_hours: List[int] = field(default_factory=lambda: [7, 8, 9, 23, 0, 1])
    peak_interval_min: int = 8
    peak_interval_max: int = 18

    # Page / Profile recycling
    page_recycle_after: int = 25
    profile_recycle_hours: int = 24

    # Paths
    screenshot_dir: str = "screenshots"
    session_dir: str = "sessions"
    db_file: str = "cita_bot.db"
    max_screenshots: int = 100

    # Stealth
    enable_stealth: bool = True
    use_proxy: bool = False
    proxy_list_file: str = "proxies.txt"

    # Circuit breaker
    captcha_rate_threshold: float = 0.4
    circuit_breaker_window_min: int = 15
    circuit_breaker_pause: int = 1800

    # Proxy intelligence
    proxy_max_captcha_rate: float = 0.3
    proxy_cooldown_sec: int = 600

    # Target
    base_url: str = "https://icp.administracionelectronica.gob.es/icpplus/index.html"
    provinces: List[str] = field(default_factory=lambda: ["BARCELONA", "MADRID"])
    tramites: List[str] = field(default_factory=lambda: [
        "POLICIA-TOMA DE HUELLA",
        "RENOVACION DE TARJETA"
    ])

    negative_keywords: List[str] = field(default_factory=lambda: [
        "no hay citas", "sin citas", "no disponible", "agotado", "no hay horarios"
    ])
    captcha_keywords: List[str] = field(default_factory=lambda: [
        "captcha", "recaptcha", "hcaptcha", "verify you are human", "robot"
    ])

    telegram_token: str = field(default_factory=lambda: os.getenv("8785288119:AAFcBt9NTKGAKHHBKwroMxxhFvb7fvUUVDBw",""))
    telegram_chat_id: str = field(default_factory=lambda: os.getenv("7291503141, ""))
    whatsapp_phone: str = field(default_factory=lambda: os.getenv("WHATSAPP_PHONE", ""))
    callmebot_apikey: str = field(default_factory=lambda: os.getenv("CALLMEBOT_APIKEY", ""))

    def __post_init__(self):
        for d in [self.screenshot_dir, self.session_dir]:
            Path(d).mkdir(exist_ok=True)

# =========================================================
# WORKER PROFILES — Fixed fingerprint per worker
# =========================================================

WORKER_PROFILES = [
    {
        "viewport": {"width": 1366, "height": 768},
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "locale": "es-ES",
        "timezone": "Europe/Madrid"
    },
    {
        "viewport": {"width": 1440, "height": 900},
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "locale": "es-AR",
        "timezone": "Europe/Paris"
    },
    {
        "viewport": {"width": 1920, "height": 1080},
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "locale": "es-MX",
        "timezone": "Atlantic/Canary"
    },
]

def get_worker_profile(worker_id: int, offset: int = 0) -> Dict:
    return WORKER_PROFILES[(worker_id + offset) % len(WORKER_PROFILES)]

# =========================================================
# ASYNC DB WRITER — Single writer, batch commits
# =========================================================

class AsyncDBWriter:
    """All DB writes go through one asyncio.Queue — no lock contention."""

    def __init__(self, db_file: str):
        self.db_file = db_file
        self._queue: asyncio.Queue = asyncio.Queue()
        self._conn: Optional[sqlite3.Connection] = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.row_factory = sqlite3.Row
            self._init_schema()
        return self._conn

    def _init_schema(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS combos (
                id              TEXT PRIMARY KEY,
                province        TEXT,
                tramite         TEXT,
                attempts        INTEGER DEFAULT 0,
                captcha_hits    INTEGER DEFAULT 0,
                retries         INTEGER DEFAULT 0,
                last_success    TEXT,
                last_error      TEXT,
                updated         TEXT
            );
            CREATE TABLE IF NOT EXISTS response_times (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                combo_id    TEXT,
                elapsed_ms  REAL,
                ts          TEXT
            );
            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ts          TEXT,
                event       TEXT,
                level       TEXT,
                worker_id   INTEGER,
                details     TEXT
            );
            CREATE TABLE IF NOT EXISTS proxy_stats (
                proxy           TEXT PRIMARY KEY,
                attempts        INTEGER DEFAULT 0,
                captchas        INTEGER DEFAULT 0,
                errors          INTEGER DEFAULT 0,
                cooldown_until  TEXT
            );
        """)
        self._conn.commit()

    async def write(self, sql: str, params: tuple = ()):
        await self._queue.put((sql, params))

    async def run(self):
        """Dedicated writer loop — batches up to 50 writes per commit."""
        conn = self._get_conn()
        batch = []
        while True:
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                batch.append(item)
                while not self._queue.empty() and len(batch) < 50:
                    batch.append(self._queue.get_nowait())
            except asyncio.TimeoutError:
                pass

            if batch:
                try:
                    for sql, params in batch:
                        conn.execute(sql, params)
                    conn.commit()
                except Exception as e:
                    logger.error(f"DB write error: {e}")
                    conn.rollback()
                finally:
                    for _ in batch:
                        self._queue.task_done()
                    batch.clear()

# =========================================================
# STATE DB — Read/write interface
# =========================================================

class StateDB:
    def __init__(self, db_file: str, writer: AsyncDBWriter):
        self.db_file = db_file
        self.writer = writer
        self._rconn: Optional[sqlite3.Connection] = None

    def _read(self) -> sqlite3.Connection:
        if self._rconn is None:
            try:
                self._rconn = sqlite3.connect(
                    f"file:{self.db_file}?mode=ro", uri=True,
                    check_same_thread=False
                )
            except:
                self._rconn = sqlite3.connect(self.db_file, check_same_thread=False)
            self._rconn.row_factory = sqlite3.Row
        return self._rconn

    async def record_attempt(self, province: str, tramite: str, worker_id: int):
        cid = f"{province}|{tramite}"
        now = datetime.utcnow().isoformat()
        await self.writer.write(
            "INSERT OR IGNORE INTO combos (id, province, tramite) VALUES (?,?,?)",
            (cid, province, tramite)
        )
        await self.writer.write(
            "UPDATE combos SET attempts=attempts+1, updated=? WHERE id=?",
            (now, cid)
        )
        await self.log_event("combo_start", "INFO", worker_id,
                             {"province": province, "tramite": tramite})

    async def record_success(self, province: str, tramite: str,
                              elapsed_ms: float, worker_id: int):
        cid = f"{province}|{tramite}"
        now = datetime.utcnow().isoformat()
        await self.writer.write(
            "UPDATE combos SET last_success=?, updated=? WHERE id=?",
            (now, now, cid)
        )
        await self.writer.write(
            "INSERT INTO response_times (combo_id, elapsed_ms, ts) VALUES (?,?,?)",
            (cid, round(elapsed_ms, 1), now)
        )

    async def record_error(self, province: str, tramite: str,
                            error: str, worker_id: int):
        cid = f"{province}|{tramite}"
        now = datetime.utcnow().isoformat()
        await self.writer.write(
            "UPDATE combos SET last_error=?, retries=retries+1, updated=? WHERE id=?",
            (error[:200], now, cid)
        )

    async def record_captcha(self, province: str, tramite: str, worker_id: int) -> int:
        cid = f"{province}|{tramite}"
        now = datetime.utcnow().isoformat()
        await self.writer.write(
            "UPDATE combos SET captcha_hits=captcha_hits+1, updated=? WHERE id=?",
            (now, cid)
        )
        await self.log_event("captcha_detected", "WARN", worker_id,
                             {"province": province, "tramite": tramite})
        try:
            row = self._read().execute(
                "SELECT captcha_hits FROM combos WHERE id=?", (cid,)
            ).fetchone()
            return row["captcha_hits"] if row else 1
        except:
            return 1

    async def log_event(self, event: str, level: str, worker_id: int, details: Dict = None):
        await self.writer.write(
            "INSERT INTO events (ts, event, level, worker_id, details) VALUES (?,?,?,?,?)",
            (
                datetime.utcnow().isoformat(),
                event, level, worker_id,
                json.dumps(details or {}, ensure_ascii=False)
            )
        )

    def get_captcha_rate(self, window_minutes: int = 15) -> float:
        """✅ Fix: captchas / attempts (accurate ratio)."""
        try:
            since = (datetime.utcnow() - timedelta(minutes=window_minutes)).isoformat()
            conn = self._read()
            attempts = conn.execute(
                "SELECT COALESCE(SUM(attempts), 0) as n FROM combos WHERE updated > ?",
                (since,)
            ).fetchone()["n"]
            captchas = conn.execute(
                "SELECT COUNT(*) as n FROM events WHERE ts > ? AND event='captcha_detected'",
                (since,)
            ).fetchone()["n"]
            return captchas / attempts if attempts > 0 else 0.0
        except:
            return 0.0

    async def update_proxy(self, proxy: str, captcha: bool = False, error: bool = False):
        await self.writer.write(
            """INSERT INTO proxy_stats (proxy, attempts) VALUES (?,1)
               ON CONFLICT(proxy) DO UPDATE SET attempts=attempts+1""",
            (proxy,)
        )
        if captcha:
            await self.writer.write(
                "UPDATE proxy_stats SET captchas=captchas+1 WHERE proxy=?", (proxy,)
            )
        if error:
            await self.writer.write(
                "UPDATE proxy_stats SET errors=errors+1 WHERE proxy=?", (proxy,)
            )

    def get_proxy_stats(self, proxy: str) -> Dict:
        try:
            row = self._read().execute(
                "SELECT * FROM proxy_stats WHERE proxy=?", (proxy,)
            ).fetchone()
            if row:
                return dict(row)
        except:
            pass
        return {"attempts": 0, "captchas": 0, "errors": 0, "cooldown_until": None}

    async def set_proxy_cooldown(self, proxy: str, seconds: int):
        until = (datetime.utcnow() + timedelta(seconds=seconds)).isoformat()
        await self.writer.write(
            "UPDATE proxy_stats SET cooldown_until=? WHERE proxy=?", (until, proxy)
        )

# =========================================================
# PROXY INTELLIGENCE ENGINE
# =========================================================

class ProxyIntelligence:
    def __init__(self, config: AppConfig, db: StateDB):
        self.config = config
        self.db = db
        self.proxies = self._load()
        self._latency: Dict[str, float] = {}

    def _load(self) -> List[str]:
        if not self.config.use_proxy:
            return []
        if os.path.exists(self.config.proxy_list_file):
            with open(self.config.proxy_list_file) as f:
                return [l.strip() for l in f if l.strip()]
        return []

    def _on_cooldown(self, proxy: str) -> bool:
        stats = self.db.get_proxy_stats(proxy)
        cd = stats.get("cooldown_until")
        if cd:
            try:
                return datetime.fromisoformat(cd) > datetime.utcnow()
            except:
                pass
        return False

    def _captcha_rate(self, proxy: str) -> float:
        s = self.db.get_proxy_stats(proxy)
        a = s.get("attempts", 0)
        return s.get("captchas", 0) / a if a > 0 else 0.0

    def _score(self, proxy: str) -> float:
        if self._on_cooldown(proxy):
            return -1.0
        latency = self._latency.get(proxy, 1.0)
        return max(0.0, 1.0 - self._captcha_rate(proxy) - latency / 10.0)

    def get_best(self) -> Optional[Dict]:
        if not self.proxies:
            return None
        scored = sorted([(self._score(p), p) for p in self.proxies], reverse=True)
        score, proxy = scored[0]
        if score < 0:
            logger.warning("All proxies on cooldown")
            return None
        return {"server": proxy, "_proxy_str": proxy}

    def record_latency(self, proxy: str, ms: float):
        self._latency[proxy] = ms / 1000.0

    async def penalize(self, proxy: str, captcha: bool = False, error: bool = False):
        await self.db.update_proxy(proxy, captcha=captcha, error=error)
        if self._captcha_rate(proxy) >= self.config.proxy_max_captcha_rate:
            await self.db.set_proxy_cooldown(proxy, self.config.proxy_cooldown_sec)
            logger.warning(f"Proxy on cooldown: {proxy[:25]}...")

# =========================================================
# CIRCUIT BREAKER
# =========================================================

class CircuitBreaker:
    def __init__(self, config: AppConfig, db: StateDB):
        self.config = config
        self.db = db
        self._open = False
        self._open_until = 0.0

    async def check(self) -> bool:
        now = time.time()
        if self._open:
            if now < self._open_until:
                logger.warning(f"Circuit OPEN — {int(self._open_until - now)}s left")
                return True
            self._open = False
            logger.info("Circuit CLOSED")

        rate = self.db.get_captcha_rate(self.config.circuit_breaker_window_min)
        if rate >= self.config.captcha_rate_threshold:
            self._open = True
            self._open_until = now + self.config.circuit_breaker_pause
            logger.warning(f"Circuit OPENED — rate={rate:.1%}")
            return True
        return False

# =========================================================
# RATE CONTROLLERS
# =========================================================

class GlobalRatePenalty:
    def __init__(self):
        self.multiplier = 1.0
        self._lock = asyncio.Lock()

    async def penalize(self):
        async with self._lock:
            self.multiplier = min(4.0, self.multiplier * 1.5)

    async def relax(self):
        async with self._lock:
            self.multiplier = max(1.0, self.multiplier * 0.95)


class WorkerRateController:
    def __init__(self, worker_id: int, global_penalty: GlobalRatePenalty):
        self.worker_id = worker_id
        self.gp = global_penalty
        self.base = 7.0
        self.current = 7.0
        self.max_delay = 60.0

    async def success(self):
        self.current = max(self.base, self.current * 0.92)
        await self.gp.relax()

    async def penalize(self, reason: str = ""):
        self.current = min(self.max_delay, self.current * 2.0)
        await self.gp.penalize()
        logger.warning(f"[W{self.worker_id}] Rate → {self.current:.1f}s [{reason}]")

    async def wait(self):
        effective = min(self.max_delay * 2, self.current * self.gp.multiplier)
        total = effective + random.uniform(0, effective * 0.15)
        logger.info(f"[W{self.worker_id}] Wait: {total:.1f}s (×{self.gp.multiplier:.2f})")
        await asyncio.sleep(total)

# =========================================================
# HUMAN BEHAVIOR ENGINE — Bezier, mis-hover, reading
# =========================================================

class HumanBehavior:

    @staticmethod
    async def delay(a: float = 0.5, b: float = 2.0):
        await asyncio.sleep(random.uniform(a, b))

    @staticmethod
    async def bezier_move(page: Page, tx: int, ty: int):
        """Quadratic bezier mouse path — not random jumps."""
        try:
            vp = page.viewport_size or {"width": 1366, "height": 768}
            sx = random.randint(50, vp["width"] - 50)
            sy = random.randint(50, vp["height"] - 50)
            cx = random.randint(min(sx, tx), max(sx, tx))
            cy = random.randint(min(sy, ty), max(sy, ty))
            steps = random.randint(10, 20)
            for i in range(steps + 1):
                t = i / steps
                x = (1-t)**2 * sx + 2*(1-t)*t * cx + t**2 * tx
                y = (1-t)**2 * sy + 2*(1-t)*t * cy + t**2 * ty
                await page.mouse.move(int(x), int(y))
                await asyncio.sleep(random.uniform(0.01, 0.04))
        except:
            pass

    @staticmethod
    async def mis_hover_click(page: Page, selector: str):
        """Hover near element then correct — simulates human imprecision."""
        try:
            el = page.locator(selector).first
            box = await el.bounding_box()
            if box:
                cx = box["x"] + box["width"] / 2
                cy = box["y"] + box["height"] / 2
                # Mis-hover first
                await page.mouse.move(
                    cx + random.randint(-20, 20),
                    cy + random.randint(-15, 15)
                )
                await asyncio.sleep(random.uniform(0.1, 0.3))
                # Correct to target via bezier
                await HumanBehavior.bezier_move(page, int(cx), int(cy))
                await asyncio.sleep(random.uniform(0.05, 0.15))
            await el.click()
        except:
            try:
                await page.click(selector)
            except:
                pass

    @staticmethod
    async def reading_pause():
        await asyncio.sleep(random.uniform(1.5, 4.0))

    @staticmethod
    async def random_scroll(page: Page):
        try:
            down = random.randint(80, 350)
            await page.evaluate(f"window.scrollBy(0, {down})")
            await asyncio.sleep(random.uniform(0.4, 1.2))
            if random.random() < 0.4:
                await page.evaluate(f"window.scrollBy(0, -{random.randint(30, down)})")
                await asyncio.sleep(random.uniform(0.2, 0.5))
        except:
            pass

# =========================================================
# TELEGRAM QUEUE — aiohttp, rate limited
# =========================================================

class TelegramQueue:
    def __init__(self, config: AppConfig):
        self.config = config
        self._queue: asyncio.Queue = asyncio.Queue()
        self._last_sent = 0.0
        self._session: Optional[aiohttp.ClientSession] = None

    async def _sess(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def enqueue(self, msg: str, buttons: List[Dict] = None):
        await self._queue.put((msg, buttons))

    async def run(self):
        while True:
            try:
                msg, buttons = await asyncio.wait_for(self._queue.get(), timeout=5)
                wait = 3.0 - (time.time() - self._last_sent)
                if wait > 0:
                    await asyncio.sleep(wait)
                await self._send(msg, buttons)
                self._last_sent = time.time()
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"TGQueue: {e}")

    async def _send(self, msg: str, buttons: List[Dict] = None):
        if not self.config.telegram_token:
            return
        try:
            sess = await self._sess()
            url = f"https://api.telegram.org/bot{self.config.telegram_token}/sendMessage"
            data = {"chat_id": self.config.telegram_chat_id,
                    "text": msg, "parse_mode": "HTML"}
            if buttons:
                data["reply_markup"] = json.dumps({"inline_keyboard": [[b] for b in buttons]})
            async with sess.post(url, data=data,
                                  timeout=aiohttp.ClientTimeout(total=15)) as r:
                r.raise_for_status()
        except Exception as e:
            logger.error(f"Telegram: {e}")

# =========================================================
# NOTIFIER
# =========================================================

class Notifier:
    def __init__(self, config: AppConfig, db: StateDB, tg: TelegramQueue):
        self.config = config
        self.db = db
        self.tg = tg
        self._last = 0.0
        self._lock = asyncio.Lock()
        self._sess: Optional[aiohttp.ClientSession] = None

    async def _get_sess(self) -> aiohttp.ClientSession:
        if not self._sess or self._sess.closed:
            self._sess = aiohttp.ClientSession()
        return self._sess

    async def alert(self, msg: str, buttons: List[Dict] = None,
                    force: bool = False, worker_id: int = -1):
        async with self._lock:
            now = time.time()
            if not force and now - self._last < self.config.alert_cooldown:
                return
            await self.tg.enqueue(msg, buttons)
            await self._whatsapp(msg)
            self._last = now
            await self.db.log_event("alert_sent", "INFO", worker_id, {"msg": msg[:200]})

    async def _whatsapp(self, msg: str):
        if not self.config.callmebot_apikey:
            return
        try:
            url = (
                f"https://api.callmebot.com/whatsapp.php"
                f"?phone={self.config.whatsapp_phone}"
                f"&text={quote(msg)}"
                f"&apikey={self.config.callmebot_apikey}"
            )
            sess = await self._get_sess()
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=15)):
                pass
        except Exception as e:
            logger.error(f"WhatsApp: {e}")

# =========================================================
# SCREENSHOT MANAGER
# =========================================================

class ScreenshotManager:
    def __init__(self, config: AppConfig):
        self.config = config

    async def save(self, page: Page, name: str, worker_id: int) -> Optional[str]:
        await self._cleanup()
        try:
            fn = f"{self.config.screenshot_dir}/w{worker_id}_{name}_{int(time.time())}.png"
            await page.screenshot(path=fn, full_page=True)
            logger.info(f"Screenshot: {fn}")
            return fn
        except Exception as e:
            logger.error(f"Screenshot: {e}")
            return None

    async def _cleanup(self):
        shots = sorted(Path(self.config.screenshot_dir).glob("*.png"), key=os.path.getmtime)
        for f in shots[:max(0, len(shots) - self.config.max_screenshots)]:
            try:
                f.unlink()
            except:
                pass

# =========================================================
# APPOINTMENT DETECTOR — Date+Time regex + enabled elements
# =========================================================

DATE_RE = re.compile(r"\b\d{1,2}[/\-]\d{1,2}[/\-]\d{4}\b")
TIME_RE = re.compile(r"\b\d{1,2}:\d{2}\b")


class AppointmentDetector:
    def __init__(self, config: AppConfig):
        self.config = config

    async def detect(self, page: Page) -> Tuple[bool, str]:
        try:
            html = await page.content()
            hl = html.lower()

            for kw in self.config.negative_keywords:
                if kw in hl:
                    return False, f"Negative: {kw}"

            has_date = bool(DATE_RE.search(html))
            has_time = bool(TIME_RE.search(html))

            # Primary: date+time+enabled clickable
            if has_date and has_time:
                for sel in [
                    "table tr td a[href]",
                    "input[value*='Seleccionar']:not([disabled])",
                    "a:has-text('Seleccionar')"
                ]:
                    try:
                        el = page.locator(sel).first
                        if await el.is_visible() and await el.is_enabled():
                            return True, f"Date+Time+{sel}"
                    except:
                        continue

            # URL state + enabled element
            url = page.url.lower()
            if any(s in url for s in ["citar", "elegir", "horario", "slot"]):
                for sel in ["input[value*='Seleccionar']:not([disabled])",
                            "a:has-text('Continuar')"]:
                    try:
                        el = page.locator(sel).first
                        if await el.is_visible() and await el.is_enabled():
                            return True, f"URL+{sel}"
                    except:
                        continue

            # DOM fallback — must be enabled
            for sel in [
                "a:has-text('Seleccionar')",
                "a:has-text('Continuar')",
                "button:has-text('Seleccionar')"
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.is_visible() and await el.is_enabled():
                        return True, f"DOM+enabled: {sel}"
                except:
                    continue

            for word in ["elija cita", "seleccionar cita", "horarios disponibles"]:
                if word in hl:
                    return True, f"Text: {word}"

        except Exception as e:
            return False, f"Error: {e}"

        return False, "No signal"

# =========================================================
# CAPTCHA DETECTOR
# =========================================================

class CaptchaDetector:
    def __init__(self, config: AppConfig):
        self.config = config

    async def detect(self, page: Page) -> bool:
        try:
            html = (await page.content()).lower()
            for kw in self.config.captcha_keywords:
                if kw in html:
                    return True
            if await page.query_selector("iframe[src*='recaptcha'],.g-recaptcha,.h-captcha"):
                return True
        except:
            pass
        return False

# =========================================================
# ASYNC BROWSER MANAGER
# =========================================================

class AsyncBrowserManager:
    def __init__(self, config: AppConfig, worker_id: int, proxy_intel: ProxyIntelligence):
        self.config = config
        self.worker_id = worker_id
        self.proxy_intel = proxy_intel
        self._profile_offset = 0
        self._profile_since = datetime.utcnow()
        self.playwright = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.task_counter = 0
        self.current_proxy: Optional[str] = None

    def _profile(self) -> Dict:
        return get_worker_profile(self.worker_id, self._profile_offset)

    def _profile_expired(self) -> bool:
        age = (datetime.utcnow() - self._profile_since).total_seconds()
        return age >= self.config.profile_recycle_hours * 3600

    async def rotate_profile(self):
        """Rotate to next profile + delete session data."""
        logger.info(f"[W{self.worker_id}] Rotating profile...")
        await self._stop_silent()
        self._profile_offset += 1
        self._profile_since = datetime.utcnow()
        ud = Path(self.config.session_dir) / f"worker_{self.worker_id}"
        shutil.rmtree(ud, ignore_errors=True)
        await self.start()

    async def start(self):
        logger.info(f"[W{self.worker_id}] Starting browser...")
        self.playwright = await async_playwright().start()
        profile = self._profile()
        ud = Path(self.config.session_dir) / f"worker_{self.worker_id}"
        ud.mkdir(exist_ok=True)

        proxy_conf = self.proxy_intel.get_best() if self.config.use_proxy else None
        self.current_proxy = proxy_conf.pop("_proxy_str", None) if proxy_conf else None

        self.context = await self.playwright.chromium.launch_persistent_context(
            user_data_dir=str(ud),
            headless=self.config.headless,
            viewport=profile["viewport"],
            user_agent=profile["user_agent"],
            locale=profile["locale"],
            timezone_id=profile["timezone"],
            proxy=proxy_conf,
            args=["--disable-blink-features=AutomationControlled"],
            ignore_https_errors=True
        )
        self.page = (
            self.context.pages[0] if self.context.pages
            else await self.context.new_page()
        )
        await self._stealth()
        logger.info(f"[W{self.worker_id}] Browser ready ✓")

    async def _stealth(self):
        if not self.config.enable_stealth:
            return
        try:
            await self.page.add_init_script("""
                Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
                Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});
                Object.defineProperty(navigator,'languages',{get:()=>['es-ES','es','en']});
                window.chrome={runtime:{}};
                const gp=WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter=function(p){
                    if(p===37445)return 'Intel Inc.';
                    if(p===37446)return 'Intel Iris OpenGL Engine';
                    return gp.apply(this,arguments);
                };
            """)
        except Exception as e:
            logger.warning(f"[W{self.worker_id}] stealth: {e}")

    async def recycle_page_if_needed(self):
        self.task_counter += 1
        if self._profile_expired():
            await self.rotate_profile()
            return
        if self.task_counter >= self.config.page_recycle_after:
            logger.info(f"[W{self.worker_id}] Recycling page")
            try:
                await self.page.close()
            except:
                pass
            self.page = await self.context.new_page()
            await self._stealth()
            self.task_counter = 0

    async def ensure_alive(self):
        try:
            await self.page.title()
        except:
            logger.warning(f"[W{self.worker_id}] Browser dead → restart")
            await self._stop_silent()
            await asyncio.sleep(2)
            await self.start()

    async def _stop_silent(self):
        for obj, m in [(self.context, "close"), (self.playwright, "stop")]:
            try:
                if obj:
                    await getattr(obj, m)()
            except:
                pass
        self.context = self.page = self.playwright = None

    async def stop(self):
        await self._stop_silent()

# =========================================================
# SMART SELECTORS
# =========================================================

class SmartSelectors:
    @staticmethod
    async def click(page: Page, selectors: List[str], timeout: int = 5000) -> bool:
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=timeout)
                await HumanBehavior.mis_hover_click(page, sel)
                return True
            except:
                continue
        return False

    @staticmethod
    async def select_label(page: Page, selectors: List[str], label: str) -> bool:
        for sel in selectors:
            try:
                await page.select_option(sel, label=re.compile(label, re.I))
                return True
            except:
                try:
                    await page.select_option(sel, label=label)
                    return True
                except:
                    continue
        return False

# =========================================================
# WATCHDOG — event loop lag, memory, stuck tasks
# =========================================================

class Watchdog:
    def __init__(self, stop_event: asyncio.Event):
        self.stop_event = stop_event
        self._started: Dict[int, float] = {}
        self._timeout = 300

    def mark_start(self, wid: int):
        self._started[wid] = time.time()

    def mark_done(self, wid: int):
        self._started.pop(wid, None)

    async def run(self):
        while not self.stop_event.is_set():
            t0 = time.time()
            await asyncio.sleep(1)
            lag = time.time() - t0 - 1.0
            if lag > 2.0:
                logger.warning(f"Event loop lag: {lag:.2f}s")

            now = time.time()
            for wid, start in list(self._started.items()):
                if now - start > self._timeout:
                    logger.error(f"[W{wid}] Stuck task: {now-start:.0f}s!")

            try:
                import resource
                mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
                if mb > 1500:
                    logger.warning(f"Memory: {mb:.0f} MB")
            except:
                pass

# =========================================================
# CRON SCHEDULER — timestamp-based, no queue.empty() race
# =========================================================

class CronScheduler:
    def __init__(self, config: AppConfig):
        self.config = config
        self._next = time.time()
        self._lock = asyncio.Lock()

    def _interval(self) -> int:
        h = datetime.now().hour
        if h in self.config.peak_hours:
            return random.randint(self.config.peak_interval_min,
                                  self.config.peak_interval_max)
        return random.randint(self.config.check_interval_min,
                              self.config.check_interval_max)

    async def get_tasks(self) -> Optional[List[Tuple[str, str]]]:
        async with self._lock:
            if time.time() >= self._next:
                iv = self._interval()
                self._next = time.time() + iv
                logger.info(f"Cycle — next in {iv}s (h={datetime.now().hour})")
                return [(p, t)
                        for p in self.config.provinces
                        for t in self.config.tramites]
            return None

# =========================================================
# ASYNC WORKER
# =========================================================

class AsyncWorker:
    def __init__(self, worker_id: int, config: AppConfig, notifier: Notifier,
                 db: StateDB, task_queue: asyncio.Queue,
                 circuit_breaker: CircuitBreaker, rate_ctrl: WorkerRateController,
                 proxy_intel: ProxyIntelligence, shot_mgr: ScreenshotManager,
                 watchdog: Watchdog, stop_event: asyncio.Event):
        self.wid = worker_id
        self.config = config
        self.notifier = notifier
        self.db = db
        self.task_queue = task_queue
        self.cb = circuit_breaker
        self.rate = rate_ctrl
        self.proxy_intel = proxy_intel
        self.shot = shot_mgr
        self.watchdog = watchdog
        self.stop_event = stop_event
        self.captcha_det = CaptchaDetector(config)
        self.apt_det = AppointmentDetector(config)
        self.browser: Optional[AsyncBrowserManager] = None

    async def _backoff(self, attempt: int):
        t = min(30.0, 2 ** attempt + random.uniform(0, 1.5))
        await asyncio.sleep(t)

    async def _handle_captcha(self, province: str, tramite: str):
        hits = await self.db.record_captcha(province, tramite, self.wid)
        await self.rate.penalize("captcha")
        if self.browser.current_proxy:
            await self.proxy_intel.penalize(self.browser.current_proxy, captcha=True)

        if hits == 1:
            await self.notifier.alert("⚠️ CAPTCHA detectado", worker_id=self.wid)
            await asyncio.sleep(60)
        elif hits <= 3:
            await self.notifier.alert(f"⚠️ CAPTCHA x{hits} — pausa", worker_id=self.wid)
            await asyncio.sleep(300)
        else:
            await self.notifier.alert(f"🚨 CAPTCHA x{hits} — rotando perfil",
                                       force=True, worker_id=self.wid)
            await self.browser.rotate_profile()
            await asyncio.sleep(600)

    async def _goto(self) -> bool:
        for attempt in range(self.config.max_retries_per_step):
            try:
                t0 = time.time()
                await self.browser.page.goto(self.config.base_url,
                                              timeout=self.config.workflow_timeout_ms)
                await self.browser.page.wait_for_load_state("domcontentloaded")
                ms = (time.time() - t0) * 1000
                if self.browser.current_proxy:
                    self.proxy_intel.record_latency(self.browser.current_proxy, ms)
                await (self.rate.penalize("slow") if ms > 10000 else self.rate.success())
                await HumanBehavior.delay()
                await HumanBehavior.random_scroll(self.browser.page)
                return True
            except PlaywrightTimeoutError:
                await self.rate.penalize("timeout")
                await self._backoff(attempt)
            except Exception as e:
                logger.warning(f"[W{self.wid}] goto {attempt+1}: {e}")
                await self._backoff(attempt)
        return False

    async def _province(self, province: str) -> bool:
        for attempt in range(self.config.max_retries_per_step):
            try:
                done = await SmartSelectors.select_label(
                    self.browser.page,
                    ["select[name='provincia']", "select#provincia", "select"],
                    province
                )
                if not done:
                    await self.browser.page.locator("select").nth(0).select_option(
                        label=re.compile(province, re.I)
                    )
                await HumanBehavior.reading_pause()
                await SmartSelectors.click(self.browser.page, [
                    "input[type='submit']", "button[type='submit']", "input[value='Aceptar']"
                ])
                await self.browser.page.wait_for_load_state("domcontentloaded")
                await HumanBehavior.reading_pause()
                return True
            except Exception as e:
                logger.warning(f"[W{self.wid}] province {attempt+1}: {e}")
                await self._backoff(attempt)
        return False

    async def _tramite(self, tramite: str) -> bool:
        for attempt in range(self.config.max_retries_per_step):
            try:
                # ✅ nth(1) — no count()
                sel = self.browser.page.locator("select").nth(1)
                if await sel.is_visible():
                    await sel.select_option(label=re.compile(tramite, re.I))
                else:
                    done = await SmartSelectors.select_label(
                        self.browser.page,
                        ["select[name='tramite']", "select"],
                        tramite
                    )
                    if not done:
                        raise Exception("No tramite selector")
                await HumanBehavior.delay()
                await SmartSelectors.click(self.browser.page, [
                    "input[type='submit']", "button[type='submit']"
                ])
                await self.browser.page.wait_for_load_state("domcontentloaded")
                await HumanBehavior.delay(2, 5)
                return True
            except Exception as e:
                logger.warning(f"[W{self.wid}] tramite {attempt+1}: {e}")
                await self._backoff(attempt)
        return False

    async def process_combo(self, province: str, tramite: str):
        tag = f"[W{self.wid}] {province}/{tramite}"
        logger.info(f"Checking: {tag}")
        self.watchdog.mark_start(self.wid)
        await self.db.record_attempt(province, tramite, self.wid)
        await self.browser.recycle_page_if_needed()
        await self.browser.ensure_alive()
        t0 = time.time()

        try:
            if not await self._goto():
                raise Exception("goto failed")

            if await self.captcha_det.detect(self.browser.page):
                await self.shot.save(self.browser.page, "captcha", self.wid)
                await self._handle_captcha(province, tramite)
                return

            if not await self._province(province):
                raise Exception("province failed")

            if await self.captcha_det.detect(self.browser.page):
                await self.shot.save(self.browser.page, "captcha_prov", self.wid)
                await self._handle_captcha(province, tramite)
                return

            if not await self._tramite(tramite):
                raise Exception("tramite failed")

            found, reason = await self.apt_det.detect(self.browser.page)
            ms = (time.time() - t0) * 1000

            if found:
                logger.info(f"✅ FOUND: {tag} — {reason}")
                await self.shot.save(self.browser.page, "found", self.wid)
                msg = (
                    f"🚨 <b>¡CITA PREVIA DETECTADA!</b> 🚨\n\n"
                    f"📍 <b>Provincia:</b> {province}\n"
                    f"📄 <b>Trámite:</b> {tramite}\n"
                    f"🔗 <a href='{self.config.base_url}'>Acceder ahora</a>\n"
                    f"🕒 {datetime.now().strftime('%H:%M:%S %d/%m/%Y')}"
                )
                await self.notifier.alert(
                    msg,
                    [{"text": "🌐 Abrir sitio", "url": self.config.base_url}],
                    worker_id=self.wid
                )
                await self.db.log_event("appointment_found", "INFO", self.wid,
                                        {"province": province, "tramite": tramite,
                                         "reason": reason, "ms": round(ms, 1)})
            else:
                logger.info(f"No appointment: {tag} — {reason}")

            await self.db.record_success(province, tramite, ms, self.wid)
            await self.rate.success()

        except PlaywrightTimeoutError:
            logger.error(f"Timeout: {tag}")
            await self.shot.save(self.browser.page, "timeout", self.wid)
            await self.rate.penalize("timeout")
            await self.db.record_error(province, tramite, "Timeout", self.wid)
            await self.browser.ensure_alive()

        except Exception as e:
            logger.error(f"Error: {tag} — {e}")
            await self.shot.save(self.browser.page, "error", self.wid)
            await self.db.record_error(province, tramite, str(e), self.wid)
            await self.browser.ensure_alive()

        finally:
            self.watchdog.mark_done(self.wid)

    async def run(self):
        self.browser = AsyncBrowserManager(self.config, self.wid, self.proxy_intel)
        await self.browser.start()
        try:
            while not self.stop_event.is_set():
                if await self.cb.check():
                    await asyncio.sleep(10)
                    continue
                try:
                    province, tramite = await asyncio.wait_for(
                        self.task_queue.get(), timeout=2
                    )
                    await self.process_combo(province, tramite)
                    self.task_queue.task_done()
                    await self.rate.wait()
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"[W{self.wid}] loop: {e}")
                    await asyncio.sleep(5)
        finally:
            await self.browser.stop()

# =========================================================
# TASK GENERATOR
# =========================================================

async def task_generator(config: AppConfig, task_queue: asyncio.Queue,
                          scheduler: CronScheduler, stop_event: asyncio.Event):
    while not stop_event.is_set():
        tasks = await scheduler.get_tasks()
        if tasks:
            for p, t in tasks:
                await task_queue.put((p, t))
        await asyncio.sleep(1)

# =========================================================
# MAIN
# =========================================================

async def async_main():
    config = AppConfig()

    db_writer = AsyncDBWriter(config.db_file)
    db = StateDB(config.db_file, db_writer)
    # Init schema before workers start
    db_writer._get_conn()

    tg = TelegramQueue(config)
    notifier = Notifier(config, db, tg)
    proxy_intel = ProxyIntelligence(config, db)
    cb = CircuitBreaker(config, db)
    gp = GlobalRatePenalty()
    shot_mgr = ScreenshotManager(config)
    scheduler = CronScheduler(config)
    task_queue: asyncio.Queue = asyncio.Queue()
    stop_event = asyncio.Event()
    watchdog = Watchdog(stop_event)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    core = [
        asyncio.create_task(db_writer.run(), name="db_writer"),
        asyncio.create_task(tg.run(), name="tg_queue"),
        asyncio.create_task(watchdog.run(), name="watchdog"),
        asyncio.create_task(task_generator(config, task_queue, scheduler, stop_event),
                            name="scheduler"),
    ]

    workers = []
    for i in range(config.max_workers):
        w = AsyncWorker(i, config, notifier, db, task_queue, cb,
                        WorkerRateController(i, gp), proxy_intel,
                        shot_mgr, watchdog, stop_event)
        workers.append(asyncio.create_task(w.run(), name=f"worker_{i}"))

    await notifier.alert("✅ CITA PREVIA BOT V7 ELITE started")
    logger.info("🚀 V7 ELITE ENGINE STARTED")

    await stop_event.wait()

    logger.info("Shutting down...")
    for t in workers + core:
        t.cancel()
    await asyncio.gather(*workers, *core, return_exceptions=True)
    await notifier.alert("🛑 Bot V7 stopped")
    logger.info("Done")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()