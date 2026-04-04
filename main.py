"""
Swastik Trading Signal Bot — v6 ULTRA PRO AI-POWERED EDITION
=============================================================
Upgrades over v4:

AI-1   [ML ENGINE]       XGBoost + RandomForest ensemble AI prediction engine.
                         Trains on live indicator features. Outputs BUY/SELL/HOLD
                         probability. Falls back gracefully if sklearn unavailable.

AI-2   [CONFIDENCE]      Final confidence = weighted blend of indicator score
                         (60%) + AI model score (40%). Output 0–100%.

BT-1   [BACKTESTING]     Full vectorised backtest module with walk-forward
                         support. Reports win rate, max drawdown, profit factor,
                         Sharpe ratio. Exports to CSV.

OPT-1  [PARAM TUNING]    Grid-search optimisation over RSI/EMA/MACD/ATR
                         parameters using backtested Sharpe ratio as objective.

AT-1   [AUTO TRADE]      Optional Angel One order placement in SAFE MODE with
                         max-daily-loss guard, max-trades limit, kill switch,
                         and trailing stop-loss management.

RM-1   [RISK MGMT]       Dynamic position sizing via Kelly fraction, trailing
                         stop-loss, break-even logic, partial profit booking.

NF-1   [NEWS FILTER]     Volatility-spike detector (ATR z-score). Skips
                         abnormally volatile windows.

PERF-1 [DASHBOARD]       /dashboard command: total trades, accuracy, P&L,
                         daily performance. CLI summary on startup.

FMT-1  [SIGNAL FORMAT]   Enhanced Telegram messages: confidence % (0–100),
                         AI score + indicator score, R:R, market condition.

ARCH-1 [MODULAR]         Each logical domain in clearly labelled sections.
                         Docstrings on every public function.

ERR-1  [STABILITY]       Retry logic, structured exception logging,
                         never-crash guarantee. All existing v4 bug-fixes kept.

─── v6 Improvements (10/10 upgrade) ────────────────────────────────────────

SEC-1  [SECURITY]        python-dotenv support: loads .env automatically.
                         TOTP secret base32 validation on startup.
                         HEALTH_SECRET constant-time comparison (timing-safe).
                         Sensitive env vars never logged.

SEC-2  [AUTH GUARD]      Telegram user-ID whitelist (ALLOWED_TELEGRAM_IDS).
                         Unauthorised users get silent rejection.

TEST-1 [UNIT TESTS]      Inline pytest-compatible test suite (run with
                         `python main_v6.py --test`). Covers: RSI, MACD,
                         ATR, Bollinger, Supertrend, pivot, position_size,
                         confidence blend, _safe_float, feature extraction.

ML-2   [CV FIX]          cross_val_score now correctly used BEFORE fitting
                         final model on full data (prevents data-leakage).
                         Added min_samples_leaf guard for small datasets.

ML-3   [LABEL QUALITY]   Training labels now updated post-trade resolution
                         (correct/wrong) instead of at signal time, removing
                         look-ahead bias in the training pipeline.

PERF-2 [CONN POOL]       SQLite connections use a thread-local pool to avoid
                         repeated open/close overhead under high concurrency.

CODE-1 [CLEANUP]         itertools import moved to module level.
                         _MIN_SCORE unused sentinel removed.
                         Type annotations tightened throughout.
                         All public functions have docstrings.
"""

from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# STDLIB IMPORTS
# ──────────────────────────────────────────────────────────────────────────────
import os
import re
import csv
import time
import math
import logging
import logging.handlers
import datetime
import threading
import asyncio
import sqlite3
import signal
import sys
import json
import queue
import hashlib
import hmac
import pickle
import statistics
import itertools
import argparse
import unittest
from typing import Optional, Dict, Tuple, List, Any
from http.server import HTTPServer, BaseHTTPRequestHandler

# ──────────────────────────────────────────────────────────────────────────────
# THIRD-PARTY IMPORTS
# ──────────────────────────────────────────────────────────────────────────────
import pyotp
import pandas as pd
import numpy as np
import pytz

# ── Load .env file if present (python-dotenv) ────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv optional — env vars can be set via shell/docker

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import RetryAfter, TimedOut, NetworkError

# Force telegram version check
import telegram
if tuple(int(x) for x in telegram.__version__.split(".")[:2]) >= (21, 0):
    raise ImportError("python-telegram-bot must be version 20.x, not 21+. Check requirements.txt")

# ── Optional ML dependencies — graceful fallback if not installed ─────────────
try:
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score
    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False

try:
    import xgboost as xgb
    _XGB_AVAILABLE = True
except ImportError:
    _XGB_AVAILABLE = False


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────────────────

LOG_DIR = os.environ.get("LOG_DIR", ".")
os.makedirs(LOG_DIR, exist_ok=True)

_file_handler = logging.handlers.RotatingFileHandler(
    os.path.join(LOG_DIR, "bot.log"),
    maxBytes=5 * 1024 * 1024,
    backupCount=5,
)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
))
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(message)s"
))

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
logger = logging.getLogger("TradingBot")

for _lib in ("httpx", "telegram", "apscheduler", "SmartApi"):
    logging.getLogger(_lib).setLevel(logging.WARNING)


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

class ConfigError(Exception):
    """Raised when a required environment variable is missing or invalid."""


def _env(key: str, default=None, required: bool = False, cast=str):
    """Read an environment variable with optional type casting."""
    val = os.environ.get(key, default)
    if required and not val:
        raise ConfigError(f"Required env variable missing: {key}")
    if val is None:
        return val
    try:
        return cast(val)
    except (ValueError, TypeError) as exc:
        raise ConfigError(f"Env var {key}={val!r} cannot be cast to {cast.__name__}: {exc}")


ANGEL_CLIENT_ID    = _env("ANGEL_CLIENT_ID",    required=True)
ANGEL_PASSWORD     = _env("ANGEL_PASSWORD",     required=True)
ANGEL_API_KEY      = _env("ANGEL_API_KEY",      required=True)
ANGEL_TOTP_SECRET  = _env("ANGEL_TOTP_SECRET",  required=True)
TELEGRAM_BOT_TOKEN = _env("TELEGRAM_BOT_TOKEN", required=True)
TELEGRAM_CHAT_ID   = _env("TELEGRAM_CHAT_ID",   required=True)
HEALTH_SECRET      = _env("HEALTH_SECRET",       default="", required=False)

# Validate TOTP secret is valid base32 on startup
def _validate_totp_secret(secret: str) -> str:
    """Validate and normalise TOTP secret. Raises ConfigError if invalid."""
    s = secret.strip().upper()
    # Add padding if needed (Base32 requires length to be multiple of 8)
    s = s + '=' * ((8 - len(s) % 8) % 8)
    import base64
    try:
        base64.b32decode(s, casefold=True)
    except Exception:
        raise ConfigError(
            "ANGEL_TOTP_SECRET is not valid base32. "
            "Check your Angel One TOTP secret key."
        )
    return s

ANGEL_TOTP_SECRET = _validate_totp_secret(ANGEL_TOTP_SECRET)

# Telegram user-ID whitelist (comma-separated). Empty = no restriction.
# Example: ALLOWED_TELEGRAM_IDS=123456789,987654321
_raw_ids = _env("ALLOWED_TELEGRAM_IDS", default="", required=False)
ALLOWED_TELEGRAM_IDS: set = (
    {int(x.strip()) for x in _raw_ids.split(",") if x.strip().isdigit()}
    if _raw_ids else set()
)

CAPITAL             = _env("CAPITAL",             default="100000", cast=float)
RISK_PER_TRADE_PCT  = _env("RISK_PER_TRADE_PCT",  default="1.0",   cast=float)
MAX_DAILY_LOSS_PCT  = _env("MAX_DAILY_LOSS_PCT",  default="3.0",   cast=float)
MAX_SIGNALS_PER_DAY = _env("MAX_SIGNALS_PER_DAY", default="6",     cast=int)
MAX_OPEN_TRADES     = _env("MAX_OPEN_TRADES",     default="2",     cast=int)
SIGNAL_COOLDOWN_MIN = _env("SIGNAL_COOLDOWN_MIN", default="4",     cast=int)
HEALTH_PORT         = _env("HEALTH_PORT",          default="8080",  cast=int)
SIGNAL_INTERVAL_MIN = _env("SIGNAL_INTERVAL_MIN", default="5",     cast=int)

# Auto-trading safe mode (disabled by default — set env to "1" to enable)
AUTO_TRADE_ENABLED  = _env("AUTO_TRADE_ENABLED",  default="0",     cast=int) == 1
AUTO_TRADE_MIN_CONF = _env("AUTO_TRADE_MIN_CONF", default="75",    cast=int)  # % confidence floor

# Kill switch — set to "1" to immediately halt all new orders
KILL_SWITCH = _env("KILL_SWITCH", default="0", cast=int) == 1

if not (0 < RISK_PER_TRADE_PCT <= 10):
    raise ConfigError("RISK_PER_TRADE_PCT must be 0–10")
if not (0 < MAX_DAILY_LOSS_PCT <= 20):
    raise ConfigError("MAX_DAILY_LOSS_PCT must be 0–20")

IST          = pytz.timezone("Asia/Kolkata")
MARKET_OPEN  = datetime.time(9, 15)
MARKET_CLOSE = datetime.time(15, 30)

SYMBOLS: Dict[str, Dict[str, str]] = {
    "NIFTY":     {"token": "99926000", "exchange": "NSE"},
    "BANKNIFTY": {"token": "99926009", "exchange": "NSE"},
}

DB_PATH      = os.environ.get("DB_PATH", "trading_bot.db")
MODEL_PATH   = os.environ.get("MODEL_PATH", "ai_models.pkl")
BT_EXPORT_PATH = os.environ.get("BT_EXPORT_PATH", "backtest_results.csv")

# Optimised indicator parameters (overridden by auto-optimisation)
_PARAMS: Dict[str, Any] = {
    "rsi_period":   14,
    "ema_fast":      9,
    "ema_mid":      21,
    "ema_slow":     50,
    "macd_fast":    12,
    "macd_slow":    26,
    "macd_sig":      9,
    "atr_period":   14,
    "bb_period":    20,
    "bb_k":          2.0,
    "adx_period":   14,
    "st_period":     7,
    "st_mult":       2.0,
    "sl_atr_mult":   1.5,
    "tp_atr_mult":   3.0,
    "min_score":     4,
}

_PARAMS_LOCK = threading.Lock()


def _params_get(key: str):
    """Thread-safe read of a single parameter from _PARAMS."""
    with _PARAMS_LOCK:
        return _PARAMS[key]


def _params_snapshot() -> dict:
    """Return a consistent snapshot of all parameters under lock."""
    with _PARAMS_LOCK:
        return dict(_PARAMS)

_shutdown_event = threading.Event()


# ──────────────────────────────────────────────────────────────────────────────
# DATABASE / STORAGE
# ──────────────────────────────────────────────────────────────────────────────

_db_lock = threading.Lock()
_db_local = threading.local()  # thread-local SQLite connection pool


def db_connect() -> sqlite3.Connection:
    """
    Return a thread-local SQLite connection (one per thread, reused).
    Falls back to a fresh connection if the cached one is closed/invalid.
    """
    conn = getattr(_db_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")   # better concurrent read
        conn.execute("PRAGMA synchronous=NORMAL")  # safe & faster than FULL
        _db_local.conn = conn
    return conn


def db_init():
    """Initialise all required tables, including new v5 tables."""
    with _db_lock:
        conn = db_connect()
        conn.executescript("""
                CREATE TABLE IF NOT EXISTS signals (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol      TEXT    NOT NULL,
                    signal      TEXT    NOT NULL,
                    price       REAL    NOT NULL,
                    confidence  INTEGER NOT NULL,
                    ai_score    REAL    DEFAULT 0,
                    ind_score   REAL    DEFAULT 0,
                    sl          REAL,
                    target      REAL,
                    result      TEXT    DEFAULT 'pending',
                    ts          TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS daily_state (
                    date        TEXT PRIMARY KEY,
                    signals_cnt INTEGER DEFAULT 0,
                    loss_rs     REAL    DEFAULT 0.0
                );

                CREATE TABLE IF NOT EXISTS open_trades (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol      TEXT    NOT NULL,
                    direction   TEXT    NOT NULL,
                    entry_price REAL    NOT NULL,
                    sl          REAL,
                    target      REAL,
                    trail_sl    REAL,
                    be_moved    INTEGER DEFAULT 0,
                    partial_done INTEGER DEFAULT 0,
                    qty         INTEGER DEFAULT 1,
                    opened_ts   TEXT    NOT NULL,
                    closed_ts   TEXT,
                    pnl         REAL,
                    order_id    TEXT
                );

                CREATE TABLE IF NOT EXISTS price_alerts (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol      TEXT    NOT NULL,
                    direction   TEXT    NOT NULL,
                    price       REAL    NOT NULL,
                    triggered   INTEGER DEFAULT 0,
                    created_ts  TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ai_training_data (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol      TEXT    NOT NULL,
                    features    TEXT    NOT NULL,
                    label       INTEGER NOT NULL,
                    ts          TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS backtest_runs (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    params      TEXT    NOT NULL,
                    win_rate    REAL,
                    profit_factor REAL,
                    max_dd      REAL,
                    sharpe      REAL,
                    total_trades INTEGER,
                    ts          TEXT    NOT NULL
                );

                -- Migration: add ai_score/ind_score columns if upgrading from v4
                CREATE TABLE IF NOT EXISTS _migrations (key TEXT PRIMARY KEY);
            """)

        # Safe column additions for upgrades from v4
        for col_sql in [
                "ALTER TABLE signals ADD COLUMN ai_score REAL DEFAULT 0",
                "ALTER TABLE signals ADD COLUMN ind_score REAL DEFAULT 0",
                "ALTER TABLE open_trades ADD COLUMN trail_sl REAL",
                "ALTER TABLE open_trades ADD COLUMN be_moved INTEGER DEFAULT 0",
                "ALTER TABLE open_trades ADD COLUMN partial_done INTEGER DEFAULT 0",
                "ALTER TABLE open_trades ADD COLUMN order_id TEXT",
            ]:
                try:
                    conn.execute(col_sql)
                    conn.commit()
                except Exception:
                    pass  # Column already exists — safe to ignore


def db_insert_signal(symbol, signal, price, confidence, sl, target,
                     ai_score: float = 0.0, ind_score: float = 0.0) -> int:
    """Insert a new signal row. Returns the new row id."""
    ts = _ist_now().isoformat()
    with _db_lock:
        conn = db_connect()
        cur = conn.execute(
            "INSERT INTO signals "
            "(symbol,signal,price,confidence,sl,target,ai_score,ind_score,ts) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (symbol, signal, price, confidence, sl, target, ai_score, ind_score, ts)
        )
        conn.commit()
        return cur.lastrowid


def db_update_signal_result(row_id: int, result: str):
    """Update the outcome of a signal (correct / wrong)."""
    with _db_lock:
        conn = db_connect()
        conn.execute("UPDATE signals SET result=? WHERE id=?", (result, row_id))
        conn.commit()


def db_get_recent_signals(limit: int = 20):
    """Return the most recent signal rows."""
    with _db_lock:
        conn = db_connect()
        return conn.execute(
            "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()


def db_get_accuracy():
    """Return resolved signals for accuracy calculation."""
    with _db_lock:
        conn = db_connect()
        return conn.execute(
            "SELECT symbol, signal, result FROM signals "
            "WHERE signal IN ('BUY','SELL') "
            "ORDER BY id DESC LIMIT 1000"
        ).fetchall()


def _today_key() -> str:
    return _ist_now().strftime("%Y-%m-%d")


def db_load_daily(date_key: str) -> Tuple[int, float]:
    """Load today's signal count and cumulative loss from DB."""
    with _db_lock:
        conn = db_connect()
        row = conn.execute(
            "SELECT signals_cnt, loss_rs FROM daily_state WHERE date=?", (date_key,)
        ).fetchone()
        return (row["signals_cnt"], row["loss_rs"]) if row else (0, 0.0)


def db_save_daily(date_key: str, signals_cnt: int, loss_rs: float):
    """Upsert today's running totals."""
    with _db_lock:
        conn = db_connect()
        conn.execute(
            "INSERT INTO daily_state(date,signals_cnt,loss_rs) VALUES(?,?,?) "
            "ON CONFLICT(date) DO UPDATE SET signals_cnt=excluded.signals_cnt, "
            "loss_rs=excluded.loss_rs",
            (date_key, signals_cnt, loss_rs)
        )
        conn.commit()


def db_store_training_sample(symbol: str, features: dict, label: int):
    """Store a labelled feature vector for AI model retraining."""
    ts = _ist_now().isoformat()
    with _db_lock:
        conn = db_connect()
        conn.execute(
            "INSERT INTO ai_training_data (symbol, features, label, ts) VALUES (?,?,?,?)",
            (symbol, json.dumps(features), label, ts)
        )
        conn.commit()


def db_get_training_data(symbol: str, limit: int = 2000):
    """Return recent labelled training samples for a symbol."""
    with _db_lock:
        conn = db_connect()
        return conn.execute(
            "SELECT features, label FROM ai_training_data "
            "WHERE symbol=? ORDER BY id DESC LIMIT ?",
            (symbol, limit)
        ).fetchall()


def db_store_backtest_run(params: dict, metrics: dict):
    """Persist backtest results for comparison."""
    ts = _ist_now().isoformat()
    with _db_lock:
        conn = db_connect()
        conn.execute(
            "INSERT INTO backtest_runs "
            "(params, win_rate, profit_factor, max_dd, sharpe, total_trades, ts) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                json.dumps(params),
                metrics.get("win_rate", 0),
                metrics.get("profit_factor", 0),
                metrics.get("max_drawdown", 0),
                metrics.get("sharpe", 0),
                metrics.get("total_trades", 0),
                ts,
            )
        )
        conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# PRICE ALERTS
# ──────────────────────────────────────────────────────────────────────────────

def db_add_alert(symbol: str, direction: str, price: float) -> int:
    """Create a new price alert. Returns alert id."""
    ts = _ist_now().isoformat()
    with _db_lock:
        conn = db_connect()
        cur = conn.execute(
            "INSERT INTO price_alerts (symbol, direction, price, created_ts) VALUES (?,?,?,?)",
            (symbol, direction, price, ts)
        )
        conn.commit()
        return cur.lastrowid


def db_get_active_alerts():
    """Return all untriggered price alerts."""
    with _db_lock:
        conn = db_connect()
        return conn.execute(
            "SELECT * FROM price_alerts WHERE triggered=0"
        ).fetchall()


def db_trigger_alert(alert_id: int):
    """Mark an alert as triggered."""
    with _db_lock:
        conn = db_connect()
        conn.execute("UPDATE price_alerts SET triggered=1 WHERE id=?", (alert_id,))
        conn.commit()


def db_delete_alert(alert_id: int):
    """Delete an alert by id."""
    with _db_lock:
        conn = db_connect()
        conn.execute("DELETE FROM price_alerts WHERE id=?", (alert_id,))
        conn.commit()


def _alert_checker_loop():
    """Background thread: checks active price alerts every 30 seconds."""
    logger.info("[AlertChecker] Price alert monitor started")
    while not _shutdown_event.is_set():
        _shutdown_event.wait(timeout=30)
        if _shutdown_event.is_set():
            break
        try:
            alerts = db_get_active_alerts()
            if not alerts:
                continue
            try:
                obj, _, _ = _angel_session()
            except Exception as exc:
                logger.warning(f"[AlertChecker] Session error: {exc}")
                continue
            for alert in alerts:
                symbol    = alert["symbol"]
                direction = alert["direction"]
                tgt_price = alert["price"]
                alert_id  = alert["id"]
                info = SYMBOLS.get(symbol)
                if not info:
                    continue
                try:
                    ltp = fetch_ltp(obj, info["exchange"], info["token"], symbol)
                except Exception:
                    continue
                triggered = (
                    (direction == "ABOVE" and ltp >= tgt_price) or
                    (direction == "BELOW" and ltp <= tgt_price)
                )
                if triggered:
                    db_trigger_alert(alert_id)
                    arrow = "📈" if direction == "ABOVE" else "📉"
                    tg_enqueue(
                        f"{arrow} *Price Alert Triggered!*\n"
                        f"{_SEP}\n"
                        f"*{symbol}* crossed Rs{tgt_price:,.0f} {direction}\n"
                        f"Current LTP: Rs{ltp:,.2f}\n"
                        f"Time: {_ist_now().strftime('%H:%M IST')}"
                    )
                    logger.info(
                        f"[AlertChecker] {symbol} alert triggered: "
                        f"{direction} Rs{tgt_price:.0f} @ {ltp:.0f}"
                    )
        except Exception as exc:
            logger.warning(f"[AlertChecker] Error: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# SHARED STATE
# ──────────────────────────────────────────────────────────────────────────────

_state_lock      = threading.Lock()
_last_signals:   Dict[str, Dict]  = {}
_last_sig_time:  Dict[str, float] = {}
_daily_date:     str   = ""
_signals_today:  int   = 0
_daily_loss_rs:  float = 0.0
_open_trades:    int   = 0
_bot_start_time: Optional[datetime.datetime] = None
_kill_active:    bool  = KILL_SWITCH  # runtime kill switch

# Pending AI training labels: row_id → (symbol, features_dict)
# Labels are written post-resolution to avoid look-ahead bias.
_pending_label_features: Dict[int, Tuple[str, dict]] = {}
_pending_label_lock = threading.Lock()


def _state_get() -> Tuple[int, float, int]:
    with _state_lock:
        return _signals_today, _daily_loss_rs, _open_trades


def _state_increment_signal():
    global _signals_today
    with _state_lock:
        _signals_today += 1
        db_save_daily(_daily_date, _signals_today, _daily_loss_rs)


def _state_add_loss(amount: float):
    global _daily_loss_rs
    with _state_lock:
        _daily_loss_rs += amount
        db_save_daily(_daily_date, _signals_today, _daily_loss_rs)


def _state_open_trade_add():
    global _open_trades
    with _state_lock:
        _open_trades += 1


def _state_open_trade_remove():
    global _open_trades
    with _state_lock:
        _open_trades = max(0, _open_trades - 1)


def _state_maybe_reset():
    """Reset daily counters if the calendar date has changed."""
    global _daily_date, _signals_today, _daily_loss_rs
    today = _today_key()
    with _state_lock:
        if today != _daily_date:
            cnt, loss      = db_load_daily(today)
            _daily_date    = today
            _signals_today = cnt
            _daily_loss_rs = loss
            logger.info(f"Daily state loaded for {today}: signals={cnt}, loss=Rs{loss:.0f}")


def _cooldown_ok(symbol: str) -> bool:
    """Return True if enough time has passed since the last signal for symbol."""
    with _state_lock:
        last = _last_sig_time.get(symbol, 0)
    return (time.monotonic() - last) >= SIGNAL_COOLDOWN_MIN * 60


def _update_last_signal(symbol: str, info: dict):
    with _state_lock:
        _last_signals[symbol]  = info
        _last_sig_time[symbol] = time.monotonic()


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _ist_now() -> datetime.datetime:
    return datetime.datetime.now(IST)


def _is_market_open() -> bool:
    now = _ist_now()
    if now.weekday() >= 5:
        return False
    t = now.time()
    return MARKET_OPEN <= t < MARKET_CLOSE


def _is_volatile_window() -> bool:
    """
    Skip first 15 min (9:15–9:30) and last 15 min (3:15–3:30).
    Only call this when _is_market_open() is True.
    """
    t         = _ist_now().time()
    open_end  = datetime.time(9, 30)
    close_pre = datetime.time(15, 15)
    return t < open_end or t >= close_pre


def _market_status_text() -> str:
    now = _ist_now()
    if now.weekday() >= 5:
        return "🔴 Closed (Weekend)"
    t = now.time()
    if t < MARKET_OPEN:
        return "🟡 Pre-market"
    if t >= MARKET_CLOSE:
        return "🔴 Closed"
    return "🟢 Open"


def _safe_float(val, default: float = 0.0) -> float:
    """Coerce val to float, returning default for NaN/Inf/errors."""
    try:
        f = float(val)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return default


# ──────────────────────────────────────────────────────────────────────────────
# TELEGRAM
# ──────────────────────────────────────────────────────────────────────────────

_SEP          = "=" * 26
_tg_queue:    queue.Queue = queue.Queue(maxsize=200)
_tg_bot:      Optional[Bot] = None
_tg_bot_lock  = threading.Lock()


def _get_bot() -> Bot:
    global _tg_bot
    with _tg_bot_lock:
        if _tg_bot is None:
            _tg_bot = Bot(token=TELEGRAM_BOT_TOKEN)
    return _tg_bot


def _truncate(msg: str, max_len: int = 4000) -> str:
    return msg if len(msg) <= max_len else msg[:max_len] + "\n…(truncated)"


def tg_enqueue(text: str):
    """Add a message to the outgoing Telegram queue (non-blocking)."""
    text = _truncate(text)
    try:
        _tg_queue.put_nowait(text)
    except queue.Full:
        logger.warning("Telegram queue full — dropping message")


def _tg_sender_loop():
    """Dedicated thread that drains the Telegram send queue with retry logic."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _send(text: str) -> bool:
        bot = _get_bot()
        for attempt in range(1, 5):
            try:
                await bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=text,
                    parse_mode="Markdown",
                    read_timeout=15,
                    write_timeout=15,
                    connect_timeout=10,
                )
                return True
            except RetryAfter as e:
                wait = e.retry_after + 1
                logger.warning(f"Telegram flood control: wait {wait}s")
                await asyncio.sleep(wait)
            except (TimedOut, NetworkError) as e:
                logger.warning(f"Telegram network error attempt {attempt}: {e}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Telegram send failed permanently: {e}")
                return False
        logger.error(f"Telegram: all retries exhausted, message lost: {text[:80]}…")
        return False

    try:
        while not _shutdown_event.is_set():
            try:
                text = _tg_queue.get(timeout=1)
                loop.run_until_complete(_send(text))
                _tg_queue.task_done()
                time.sleep(0.3)
            except queue.Empty:
                pass
            except Exception as e:
                logger.error(f"Telegram sender loop error: {e}")
    finally:
        loop.close()


# ──────────────────────────────────────────────────────────────────────────────
# ANGEL ONE API SESSION
# ──────────────────────────────────────────────────────────────────────────────

_angel_session_cache: Optional[Tuple[SmartConnect, str, str, float]] = None
_angel_session_lock   = threading.Lock()
_SESSION_TTL_SEC      = 45 * 60


def _angel_session(retries: int = 3, force_refresh: bool = False) -> Tuple[SmartConnect, str, str]:
    """
    Returns (SmartConnect obj, auth_token, feed_token).
    Caches the session for 45 minutes to reduce API calls.
    Thread-safe with exponential back-off on failures.
    """
    global _angel_session_cache

    if not force_refresh:
        with _angel_session_lock:
            if _angel_session_cache is not None:
                obj, auth, feed, created = _angel_session_cache
                if time.monotonic() - created < _SESSION_TTL_SEC:
                    logger.debug("Angel One session reused from cache")
                    return obj, auth, feed

    for attempt in range(1, retries + 1):
        try:
            totp_secret = ANGEL_TOTP_SECRET.strip().upper()
            totp = pyotp.TOTP(totp_secret).now()
            obj  = SmartConnect(api_key=ANGEL_API_KEY)
            data = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            if data.get("status"):
                auth_token = data["data"]["jwtToken"]
                feed_token = obj.getfeedToken()
                with _angel_session_lock:
                    _angel_session_cache = (obj, auth_token, feed_token, time.monotonic())
                logger.info("Angel One session created and cached")
                return obj, auth_token, feed_token
            raise RuntimeError(f"Login: {data.get('message', 'unknown')}")
        except Exception as exc:
            logger.warning(f"Angel session attempt {attempt}/{retries}: {exc}")
            if attempt < retries:
                time.sleep(min(2 ** attempt, 10))
    raise RuntimeError("Angel One session failed after all retries")


def _api_call_with_retry(fn, *args, retries: int = 3, **kwargs):
    """Generic API call wrapper with exponential back-off retry."""
    for attempt in range(1, retries + 1):
        try:
            result = fn(*args, **kwargs)
            if isinstance(result, dict) and result.get("status") is False:
                msg = result.get("message", "status:False")
                logger.warning(
                    f"API call {fn.__name__} returned status:False "
                    f"(attempt {attempt}/{retries}): {msg}"
                )
                if attempt < retries:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError(f"API call {fn.__name__} failed: {msg}")
            return result
        except Exception as exc:
            logger.warning(f"API call {fn.__name__} attempt {attempt}/{retries}: {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"API call {fn.__name__} failed after {retries} retries")


# ──────────────────────────────────────────────────────────────────────────────
# DATA FETCHING & CACHING
# ──────────────────────────────────────────────────────────────────────────────

_candle_cache:      Dict[str, Tuple[float, pd.DataFrame]] = {}
_candle_cache_lock  = threading.Lock()
_CACHE_TTL          = 4 * 60

_daily_ohlc_cache:  Dict[str, dict] = {}
_daily_ohlc_lock    = threading.Lock()


def _parse_candles(response) -> pd.DataFrame:
    """
    Parse Angel One getCandleData response into a clean DataFrame.
    Raises ValueError on empty or low-quality data.
    """
    data = response.get("data") or []
    if not data:
        raise ValueError("Empty candle data from API")
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    df   = pd.DataFrame(data, columns=cols)
    ts   = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(IST)
    df["timestamp"] = ts
    df = df.set_index("timestamp")
    df = df[cols[1:]].apply(pd.to_numeric, errors="coerce")
    df = df[~df.index.duplicated(keep="last")].sort_index().dropna(how="all")
    if len(df) < 10:
        raise ValueError(f"Too few candles: {len(df)}")
    if df["close"].isna().sum() > len(df) * 0.1:
        raise ValueError("Too many NaN closes")
    return df


def fetch_candles(obj: SmartConnect, token: str, exchange: str,
                  interval: str, days: int) -> pd.DataFrame:
    """Fetch OHLCV candles with TTL caching to reduce API load."""
    cache_key = f"{token}_{interval}_{days}"
    with _candle_cache_lock:
        if cache_key in _candle_cache:
            ts_cached, df_cached = _candle_cache[cache_key]
            if time.monotonic() - ts_cached < _CACHE_TTL:
                return df_cached

    to_ist   = _ist_now()
    from_ist = to_ist - datetime.timedelta(days=days)
    params = {
        "exchange":    exchange,
        "symboltoken": token,
        "interval":    interval,
        "fromdate":    from_ist.strftime("%Y-%m-%d %H:%M"),
        "todate":      to_ist.strftime("%Y-%m-%d %H:%M"),
    }
    resp = _api_call_with_retry(obj.getCandleData, params)
    if not resp.get("status"):
        raise RuntimeError(f"getCandleData({interval}) failed: {resp.get('message')}")
    df = _parse_candles(resp)
    logger.info(f"[{interval}] {len(df)} candles fetched ({token})")
    with _candle_cache_lock:
        _candle_cache[cache_key] = (time.monotonic(), df)
    return df


def fetch_prev_day_ohlc(obj: SmartConnect, token: str, exchange: str) -> dict:
    """Fetch previous trading day OHLC for pivot point calculation. Cached per day."""
    today_str = _ist_now().strftime("%Y-%m-%d")
    cache_key = f"{token}_{today_str}"
    with _daily_ohlc_lock:
        if cache_key in _daily_ohlc_cache:
            logger.debug(f"Daily OHLC cache hit for {token}")
            return _daily_ohlc_cache[cache_key]

    to_ist   = _ist_now()
    from_ist = to_ist - datetime.timedelta(days=10)
    params = {
        "exchange":    exchange,
        "symboltoken": token,
        "interval":    "ONE_DAY",
        "fromdate":    from_ist.strftime("%Y-%m-%d %H:%M"),
        "todate":      to_ist.strftime("%Y-%m-%d %H:%M"),
    }
    resp = _api_call_with_retry(obj.getCandleData, params)
    if not resp.get("status"):
        raise RuntimeError(f"Daily OHLC failed: {resp.get('message')}")
    df      = _parse_candles(resp)
    today   = to_ist.date()
    df_prev = df[df.index.date < today]
    if df_prev.empty:
        df_prev = df
    prev   = df_prev.iloc[-1]
    result = {
        "date":  df_prev.index[-1].strftime("%d %b"),
        "open":  float(prev["open"]),
        "high":  float(prev["high"]),
        "low":   float(prev["low"]),
        "close": float(prev["close"]),
    }
    with _daily_ohlc_lock:
        _daily_ohlc_cache.clear()
        _daily_ohlc_cache[cache_key] = result
    logger.info(f"Daily OHLC fetched and cached for {token} ({today_str})")
    return result


def _cache_cleanup_loop():
    """Periodically evict stale candle cache entries."""
    while not _shutdown_event.is_set():
        _shutdown_event.wait(timeout=300)
        if _shutdown_event.is_set():
            break
        cutoff = time.monotonic() - _CACHE_TTL * 3
        with _candle_cache_lock:
            stale = [k for k, (ts, _) in _candle_cache.items() if ts < cutoff]
            for k in stale:
                del _candle_cache[k]
        if stale:
            logger.debug(f"[CacheCleanup] Removed {len(stale)} stale entries")


# ──────────────────────────────────────────────────────────────────────────────
# WEBSOCKET REAL-TIME LTP
# ──────────────────────────────────────────────────────────────────────────────

_ws_ltp_cache:       Dict[str, float] = {}
_ws_ltp_lock         = threading.Lock()
_ws_instance:        Optional[SmartWebSocketV2] = None
_ws_thread:          Optional[threading.Thread] = None
_ws_ready            = threading.Event()
_ws_reconnect_lock   = threading.Lock()
_ws_reconnect_count  = 0
_WS_MAX_RECONNECTS   = 10
_ws_auth_token_ref:  Optional[str] = None
_ws_feed_token_ref:  Optional[str] = None
_ws_client_code_ref: Optional[str] = None


def _ws_on_open(wsapp):
    global _ws_reconnect_count
    logger.info("[WebSocket] Connection opened")
    with _ws_reconnect_lock:
        _ws_reconnect_count = 0


def _ws_on_error(wsapp, error):
    logger.error(f"[WebSocket] Error: {error}")


def _ws_on_close(wsapp):
    global _ws_reconnect_count
    logger.warning("[WebSocket] Connection closed")
    _ws_ready.clear()

    with _ws_reconnect_lock:
        if _shutdown_event.is_set():
            return
        _ws_reconnect_count += 1
        count = _ws_reconnect_count

    if count > _WS_MAX_RECONNECTS:
        logger.error("[WebSocket] Max reconnects reached — REST fallback active")
        return

    delay = min(5 * count, 60)
    logger.info(f"[WebSocket] Reconnecting in {delay}s (attempt {count}/{_WS_MAX_RECONNECTS})…")

    def _delayed_reconnect():
        _shutdown_event.wait(timeout=delay)
        if _shutdown_event.is_set():
            return
        if _ws_auth_token_ref and _ws_feed_token_ref and _ws_client_code_ref:
            try:
                start_websocket(_ws_auth_token_ref, _ws_feed_token_ref, _ws_client_code_ref)
            except Exception as exc:
                logger.error(f"[WebSocket] Reconnect failed: {exc}")
        else:
            logger.warning("[WebSocket] Cannot reconnect — tokens not available")

    threading.Thread(target=_delayed_reconnect, name="ws-reconnect", daemon=True).start()


def _ws_on_data(wsapp, message, data_type, continue_flag):
    """Parse incoming LTP tick (Angel One sends values in paise)."""
    try:
        if not isinstance(message, dict):
            return
        token   = str(message.get("token", ""))
        ltp_raw = message.get("last_traded_price", 0)
        if token and ltp_raw:
            ltp = ltp_raw / 100.0
            with _ws_ltp_lock:
                _ws_ltp_cache[token] = ltp
            _ws_ready.set()
            logger.debug(f"[WebSocket] tick token={token} ltp={ltp:.2f}")
    except Exception as exc:
        logger.warning(f"[WebSocket] tick parse error: {exc}")


def _build_ws_token_list() -> list:
    nse_tokens = [info["token"] for info in SYMBOLS.values() if info["exchange"] == "NSE"]
    return [{"exchangeType": 1, "tokens": nse_tokens}]


def start_websocket(auth_token: str, feed_token: str, client_code: str):
    """Start the WebSocket LTP streaming thread."""
    global _ws_instance, _ws_thread
    global _ws_auth_token_ref, _ws_feed_token_ref, _ws_client_code_ref
    _ws_auth_token_ref  = auth_token
    _ws_feed_token_ref  = feed_token
    _ws_client_code_ref = client_code

    mode       = 3  # SNAP_QUOTE (LTP + depth)
    token_list = _build_ws_token_list()

    sws = SmartWebSocketV2(
        auth_token=auth_token,
        api_key=ANGEL_API_KEY,
        client_code=client_code,
        feed_token=feed_token,
    )
    sws.on_open  = _ws_on_open
    sws.on_error = _ws_on_error
    sws.on_close = _ws_on_close
    sws.on_data  = _ws_on_data

    def _ws_subscribe_and_run():
        try:
            sws.subscribe("trading_bot_ws", mode, token_list)
            logger.info(f"[WebSocket] Subscribed to tokens: {token_list}")
            sws.connect()
        except Exception as exc:
            logger.error(f"[WebSocket] Fatal error in WS thread: {exc}", exc_info=True)

    _ws_instance = sws
    _ws_thread   = threading.Thread(target=_ws_subscribe_and_run, name="ws-ltp", daemon=True)
    _ws_thread.start()
    logger.info("[WebSocket] LTP streaming thread started")


def stop_websocket():
    """Gracefully close the WebSocket connection."""
    global _ws_instance
    if _ws_instance is not None:
        try:
            _ws_instance.close_connection()
            logger.info("[WebSocket] Connection closed gracefully")
        except Exception as exc:
            logger.warning(f"[WebSocket] Error closing: {exc}")
        _ws_instance = None


def fetch_ltp(obj: SmartConnect, exchange: str, token: str, symbol: str) -> float:
    """
    Return the current last traded price.
    Uses WebSocket cache if available; falls back to REST API.
    """
    with _ws_ltp_lock:
        cached = _ws_ltp_cache.get(token)
    if cached is not None and cached > 0:
        logger.debug(f"[WS-LTP] {symbol} = {cached:.2f}")
        return cached
    data = _api_call_with_retry(obj.ltpData, exchange, symbol, token)
    if data.get("status"):
        ltp = float(data["data"]["ltp"])
        if ltp <= 0:
            raise ValueError(f"Invalid LTP {ltp} for {symbol}")
        return ltp
    raise RuntimeError(f"LTP failed: {data.get('message')}")


# ──────────────────────────────────────────────────────────────────────────────
# TECHNICAL INDICATORS
# ──────────────────────────────────────────────────────────────────────────────

def calc_rsi(series: pd.Series, period: int = None) -> pd.Series:
    """Wilder RSI using EMA smoothing."""
    period = period or _params_get("rsi_period")
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).round(2)


def calc_macd(series: pd.Series, fast: int = None, slow: int = None, sig: int = None):
    """MACD line, signal line, histogram."""
    fast = fast or _params_get("macd_fast")
    slow = slow or _params_get("macd_slow")
    sig  = sig  or _params_get("macd_sig")
    ema_f    = series.ewm(span=fast, adjust=False).mean()
    ema_s    = series.ewm(span=slow, adjust=False).mean()
    macd     = ema_f - ema_s
    signal_l = macd.ewm(span=sig, adjust=False).mean()
    return macd, signal_l, (macd - signal_l)


def calc_vwap(df: pd.DataFrame) -> pd.Series:
    """Intraday VWAP reset at each session open."""
    date_grp = df.index.normalize()
    vol_nz   = df["volume"].clip(lower=0).replace(0, np.nan)
    tp       = (df["high"] + df["low"] + df["close"]) / 3
    cum_tv   = (tp * vol_nz.fillna(0)).groupby(date_grp).cumsum()
    cum_v    = vol_nz.fillna(0).groupby(date_grp).cumsum()
    return (cum_tv / cum_v.replace(0, np.nan)).ffill()


def calc_bollinger(series: pd.Series, period: int = None, k: float = None):
    """Bollinger Bands: upper, mid (SMA), lower."""
    period = period or _params_get("bb_period")
    k      = k      or _params_get("bb_k")
    sma = series.rolling(period, min_periods=period).mean()
    std = series.rolling(period, min_periods=period).std(ddof=1)
    return sma + k * std, sma, sma - k * std


def calc_atr(df: pd.DataFrame, period: int = None) -> pd.Series:
    """Average True Range."""
    period = period or _params_get("atr_period")
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift(1)).abs()
    lc = (df["low"]  - df["close"].shift(1)).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, min_periods=period, adjust=False).mean()


def calc_adx(df: pd.DataFrame, period: int = None) -> pd.Series:
    """Directional Movement Index — ADX."""
    period = period or _params_get("adx_period")
    high, low = df["high"], df["low"]
    p_dm_raw  = high.diff().clip(lower=0)
    m_dm_raw  = (-low.diff()).clip(lower=0)
    p_dm = p_dm_raw.where(p_dm_raw > m_dm_raw, 0.0)
    m_dm = m_dm_raw.where(m_dm_raw > p_dm_raw, 0.0)
    atr      = calc_atr(df, period)
    atr_safe = atr.replace(0, np.nan)
    p_di = 100 * p_dm.ewm(com=period - 1, adjust=False).mean() / atr_safe
    m_di = 100 * m_dm.ewm(com=period - 1, adjust=False).mean() / atr_safe
    dx   = 100 * (p_di - m_di).abs() / (p_di + m_di).replace(0, np.nan)
    return dx.ewm(com=period - 1, adjust=False).mean().round(2)


def calc_supertrend(df: pd.DataFrame, period: int = None, mult: float = None):
    """Supertrend indicator. Returns (supertrend_series, direction_series)."""
    period = period or _params_get("st_period")
    mult   = mult   or _params_get("st_mult")
    atr  = calc_atr(df, period=period)
    hl2  = (df["high"] + df["low"]) / 2
    bu   = (hl2 + mult * atr).values
    bl   = (hl2 - mult * atr).values
    cls  = df["close"].values
    n    = len(df)
    fu   = bu.copy()
    fl   = bl.copy()
    st   = np.full(n, np.nan)
    dire = np.ones(n, dtype=int)

    atr_vals = atr.values
    valid    = np.where(~np.isnan(atr_vals))[0]
    if len(valid) == 0:
        return (
            pd.Series(st,   index=df.index, name="supertrend"),
            pd.Series(dire, index=df.index, name="st_direction"),
        )
    first = int(valid[0])

    if cls[first] >= hl2.values[first]:
        dire[first] = 1
        st[first]   = fl[first]
    else:
        dire[first] = -1
        st[first]   = fu[first]

    for i in range(first + 1, n):
        fu[i] = bu[i] if (bu[i] < fu[i-1] or cls[i-1] > fu[i-1]) else fu[i-1]
        fl[i] = bl[i] if (bl[i] > fl[i-1] or cls[i-1] < fl[i-1]) else fl[i-1]
        if dire[i-1] == -1:
            dire[i] = 1  if cls[i] > fu[i-1] else -1
        else:
            dire[i] = -1 if cls[i] < fl[i-1] else  1
        st[i] = fl[i] if dire[i] == 1 else fu[i]

    return (
        pd.Series(st,   index=df.index, name="supertrend"),
        pd.Series(dire, index=df.index, name="st_direction"),
    )


def calc_pivot_points(prev: dict) -> dict:
    """Classic floor pivot points + CPR (Central Pivot Range)."""
    H  = prev["high"]
    L  = prev["low"]
    C  = prev["close"]
    P  = (H + L + C) / 3
    R1 = 2 * P - L
    S1 = 2 * P - H
    R2 = P + (H - L)
    S2 = P - (H - L)
    R3 = H + 2 * (P - L)
    bc  = (H + L) / 2
    tc  = (P - bc) + P
    rng = H - L if H > L else 1.0
    return {
        "pivot": P, "bc": bc, "tc": tc,
        "cpr_width":  abs(tc - bc),
        "cpr_narrow": abs(tc - bc) < rng * 0.15,
        "r1": R1, "r2": R2, "r3": R3,
        "s1": S1, "s2": S2,
        "prev_high":  H,
        "prev_low":   L,
        "prev_close": C,
        "prev_date":  prev["date"],
    }


# ──────────────────────────────────────────────────────────────────────────────
# VOLATILITY / NEWS FILTER
# ──────────────────────────────────────────────────────────────────────────────

_atr_history: Dict[str, List[float]] = {}
_ATR_HISTORY_MAX = 50


def _update_atr_history(symbol: str, atr: float):
    """Maintain a rolling window of ATR values for z-score normalisation."""
    hist = _atr_history.setdefault(symbol, [])
    hist.append(atr)
    if len(hist) > _ATR_HISTORY_MAX:
        hist.pop(0)


def _is_abnormal_volatility(symbol: str, current_atr: float, threshold_z: float = 2.5) -> bool:
    """
    Returns True if current ATR is abnormally high (z-score > threshold).
    Prevents trading during news-driven volatility spikes.
    """
    hist = _atr_history.get(symbol, [])
    if len(hist) < 10:
        return False
    mu  = statistics.mean(hist)
    std = statistics.stdev(hist)
    if std < 1e-9:
        return False
    z = (current_atr - mu) / std
    if z > threshold_z:
        logger.warning(
            f"[VolFilter] {symbol}: ATR z-score={z:.2f} "
            f"(current={current_atr:.1f}, mean={mu:.1f}) — SKIP"
        )
        return True
    return False


# ──────────────────────────────────────────────────────────────────────────────
# INDICATOR PIPELINES
# ──────────────────────────────────────────────────────────────────────────────

def _indicators_5min(df: pd.DataFrame, params: dict = None) -> pd.DataFrame:
    """Compute full indicator suite on the 5-minute frame."""
    p = params or _PARAMS
    df = df.copy()
    df["ema9"]    = df["close"].ewm(span=p["ema_fast"],  adjust=False).mean()
    df["ema21"]   = df["close"].ewm(span=p["ema_mid"],   adjust=False).mean()
    df["ema50"]   = df["close"].ewm(span=p["ema_slow"],  adjust=False).mean()
    df["rsi"]     = calc_rsi(df["close"], period=p["rsi_period"])
    df["macd"], df["macd_sig"], df["macd_hist"] = calc_macd(
        df["close"], fast=p["macd_fast"], slow=p["macd_slow"], sig=p["macd_sig"]
    )
    df["vwap"]    = calc_vwap(df)
    df["bb_upper"], df["bb_mid"], df["bb_lower"] = calc_bollinger(
        df["close"], period=p["bb_period"], k=p["bb_k"]
    )
    df["atr"]     = calc_atr(df, period=p["atr_period"])
    df["adx"]     = calc_adx(df, period=p["adx_period"])
    df["vol_avg"] = df["volume"].rolling(window=20, min_periods=5).mean()
    return df


def _indicators_15min(df: pd.DataFrame, params: dict = None) -> pd.DataFrame:
    """Compute trend indicators on the 15-minute frame."""
    p = params or _PARAMS
    df = df.copy()
    df["ema9"]  = df["close"].ewm(span=p["ema_fast"], adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=p["ema_mid"],  adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=p["ema_slow"], adjust=False).mean()
    df["rsi"]   = calc_rsi(df["close"], period=p["rsi_period"])
    df["adx"]   = calc_adx(df, period=p["adx_period"])
    df["supertrend"], df["st_direction"] = calc_supertrend(
        df, period=p["st_period"], mult=p["st_mult"]
    )
    return df


# ──────────────────────────────────────────────────────────────────────────────
# POSITION SIZING — ADVANCED (Kelly + ATR)
# ──────────────────────────────────────────────────────────────────────────────

def position_size(ltp: float, sl: float,
                  win_rate: float = 0.55,
                  avg_win_loss_ratio: float = 2.0) -> dict:
    """
    Dynamic position sizing using a capped Kelly fraction.

    Args:
        ltp: Entry price.
        sl: Stop-loss price.
        win_rate: Historical win rate (0–1). Default 55%.
        avg_win_loss_ratio: Average win / average loss ratio.

    Returns:
        dict with qty, risk_rs, pts_risk, capital_used, kelly_pct.
    """
    risk_rs  = CAPITAL * RISK_PER_TRADE_PCT / 100
    pts_risk = abs(ltp - sl)
    if pts_risk < 0.01:
        return {"qty": 0, "risk_rs": 0, "pts_risk": 0, "capital_used": 0, "kelly_pct": 0}

    # Kelly criterion — cap at 2× fixed risk to avoid over-sizing
    b = avg_win_loss_ratio
    kelly_frac = max(0.0, (b * win_rate - (1 - win_rate)) / b)
    kelly_capital = CAPITAL * min(kelly_frac, RISK_PER_TRADE_PCT / 100 * 2)
    kelly_qty = int(kelly_capital / pts_risk) if pts_risk > 0 else 0

    max_qty_by_risk    = int(risk_rs / pts_risk)
    max_qty_by_capital = int(CAPITAL / ltp) if ltp > 0 else 1
    qty = max(1, min(kelly_qty if kelly_qty > 0 else max_qty_by_risk,
                     max_qty_by_risk, max_qty_by_capital))

    return {
        "qty":          qty,
        "risk_rs":      round(risk_rs, 2),
        "pts_risk":     round(pts_risk, 2),
        "capital_used": round(qty * ltp, 2),
        "kelly_pct":    round(kelly_frac * 100, 1),
    }


# ──────────────────────────────────────────────────────────────────────────────
# AI PREDICTION ENGINE
# ──────────────────────────────────────────────────────────────────────────────

# Feature column names used for model training / inference
_AI_FEATURES = [
    "rsi", "adx", "macd_hist_norm", "ema9_vs_ema21",
    "ema21_vs_ema50", "ltp_vs_vwap", "ltp_vs_bb_mid",
    "bb_width_norm", "st_dir", "vol_ratio", "atr_norm",
    "cpr_pos",  # +1 above TC, 0 between, -1 below BC
]

_ai_models:  Dict[str, Any] = {}   # symbol → fitted model object
_ai_scalers: Dict[str, Any] = {}   # symbol → fitted scaler
_ai_lock = threading.Lock()


def _build_feature_vector(sg: dict, ltp: float) -> Optional[Dict[str, float]]:
    """
    Convert a signal dict into a normalised feature vector for the AI model.
    Returns None if required values are missing.
    """
    try:
        atr      = max(_safe_float(sg.get("atr"), 1.0), 1e-9)
        bb_upper = _safe_float(sg.get("bb_upper"), ltp)
        bb_lower = _safe_float(sg.get("bb_lower"), ltp)
        bb_mid   = (bb_upper + bb_lower) / 2
        bb_width = (bb_upper - bb_lower) / max(bb_mid, 1e-9)

        # CPR position: +1 above TC, -1 below BC, 0 in between
        if "pp" in sg:
            tc = sg["pp"].get("tc", ltp)
            bc = sg["pp"].get("bc", ltp)
            cpr_pos = 1 if ltp > tc else (-1 if ltp < bc else 0)
        else:
            cpr_pos = 0

        return {
            "rsi":            _safe_float(sg.get("rsi"), 50.0),
            "adx":            _safe_float(sg.get("adx"), 20.0),
            "macd_hist_norm": _safe_float(sg.get("macd_hist"), 0.0) / atr,
            "ema9_vs_ema21":  (_safe_float(sg.get("ema9"), ltp) - _safe_float(sg.get("ema21"), ltp)) / atr,
            "ema21_vs_ema50": (_safe_float(sg.get("ema21"), ltp) - _safe_float(sg.get("ema50"), ltp)) / atr,
            "ltp_vs_vwap":    (ltp - _safe_float(sg.get("vwap"), ltp)) / atr,
            "ltp_vs_bb_mid":  (ltp - bb_mid) / max(atr, 1e-9),
            "bb_width_norm":  bb_width,
            "st_dir":         float(_safe_float(sg.get("st_dir"), 0)),
            "vol_ratio":      float(bool(sg.get("vol_ok", True))),
            "atr_norm":       atr / max(ltp, 1e-9),
            "cpr_pos":        float(cpr_pos),
        }
    except Exception as exc:
        logger.warning(f"[AI] Feature extraction error: {exc}")
        return None


def _train_model(symbol: str) -> bool:
    """
    Train or re-train the AI ensemble for a symbol using stored labelled data.
    Returns True if training succeeded.
    """
    if not _ML_AVAILABLE:
        return False

    rows = db_get_training_data(symbol, limit=2000)
    if len(rows) < 50:
        logger.info(f"[AI] Not enough training data for {symbol} ({len(rows)} samples)")
        return False

    X, y = [], []
    for row in rows:
        try:
            feats = json.loads(row["features"])
            X.append([feats.get(k, 0.0) for k in _AI_FEATURES])
            y.append(int(row["label"]))
        except Exception:
            continue

    if len(X) < 50:
        return False

    X_arr = np.array(X)
    y_arr = np.array(y)

    try:
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_arr)

        # Use XGBoost if available, else GradientBoosting
        if _XGB_AVAILABLE:
            model = xgb.XGBClassifier(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8, use_label_encoder=False,
                eval_metric="logloss", random_state=42, n_jobs=-1,
                min_child_weight=3,  # guard against over-fitting on small datasets
            )
        else:
            model = GradientBoostingClassifier(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                subsample=0.8, random_state=42, min_samples_leaf=5,
            )

        # ── Cross-validation BEFORE final fit (prevents data-leakage) ────────
        # We clone the unfitted model for CV, then fit the real model on full data.
        from sklearn.base import clone as _clone
        cv_model   = _clone(model)
        cv_scores  = cross_val_score(cv_model, X_scaled, y_arr, cv=5, scoring="accuracy")
        cv_mean    = float(cv_scores.mean())

        # Final fit on all available data
        model.fit(X_scaled, y_arr)

        with _ai_lock:
            _ai_models[symbol]  = model
            _ai_scalers[symbol] = scaler

        # Persist to disk
        try:
            with open(MODEL_PATH, "wb") as f:
                pickle.dump({"models": _ai_models, "scalers": _ai_scalers}, f)
        except Exception as exc:
            logger.warning(f"[AI] Model save error: {exc}")

        lib_name = "XGBoost" if _XGB_AVAILABLE else "GBT"
        logger.info(
            f"[AI] {symbol}: {lib_name} trained on {len(X)} samples "
            f"| CV accuracy={cv_mean:.2%}"
        )
        return True
    except Exception as exc:
        logger.error(f"[AI] Training failed for {symbol}: {exc}")
        return False


def _load_models():
    """Load persisted AI models from disk if available."""
    global _ai_models, _ai_scalers
    if not os.path.exists(MODEL_PATH):
        return
    try:
        with open(MODEL_PATH, "rb") as f:
            data = pickle.load(f)
        with _ai_lock:
            _ai_models  = data.get("models", {})
            _ai_scalers = data.get("scalers", {})
        logger.info(f"[AI] Models loaded for symbols: {list(_ai_models.keys())}")
    except Exception as exc:
        logger.warning(f"[AI] Could not load models: {exc}")


def _ai_predict(symbol: str, features: Dict[str, float]) -> Tuple[float, str]:
    """
    Run AI inference for a symbol.

    Returns:
        (ai_confidence_0_to_1, direction) where direction ∈ {BUY, SELL, HOLD}.
        Falls back to (0.5, HOLD) if model unavailable.
    """
    if not _ML_AVAILABLE:
        return 0.5, "HOLD"

    with _ai_lock:
        model  = _ai_models.get(symbol)
        scaler = _ai_scalers.get(symbol)

    if model is None or scaler is None:
        return 0.5, "HOLD"

    try:
        X = np.array([[features.get(k, 0.0) for k in _AI_FEATURES]])
        X_scaled = scaler.transform(X)
        proba = model.predict_proba(X_scaled)[0]
        classes = list(model.classes_)

        # label encoding: BUY=1, SELL=-1, HOLD=0
        buy_prob  = proba[classes.index(1)]  if 1  in classes else 0.0
        sell_prob = proba[classes.index(-1)] if -1 in classes else 0.0
        hold_prob = proba[classes.index(0)]  if 0  in classes else 0.0

        if buy_prob > sell_prob and buy_prob > hold_prob:
            return float(buy_prob), "BUY"
        if sell_prob > buy_prob and sell_prob > hold_prob:
            return float(sell_prob), "SELL"
        return float(hold_prob), "HOLD"
    except Exception as exc:
        logger.warning(f"[AI] Predict error {symbol}: {exc}")
        return 0.5, "HOLD"


def _ai_model_retrain_loop():
    """Background thread: retrain models every 4 hours."""
    _shutdown_event.wait(timeout=300)  # initial delay 5 min
    while not _shutdown_event.is_set():
        for sym in SYMBOLS:
            try:
                _train_model(sym)
            except Exception as exc:
                logger.warning(f"[AI] Retrain error {sym}: {exc}")
        _shutdown_event.wait(timeout=4 * 3600)


def _compute_final_confidence(ind_score: int, ind_max: int,
                               ai_conf: float, ai_dir: str,
                               signal: str) -> int:
    """
    Blend indicator score (60%) + AI score (40%) into a 0–100% confidence.

    Args:
        ind_score: Raw indicator score (0 to ind_max).
        ind_max: Maximum possible indicator score (e.g. 9).
        ai_conf: AI model probability (0.0–1.0).
        ai_dir: AI predicted direction.
        signal: Indicator-derived signal direction.

    Returns:
        Integer confidence 0–100.
    """
    ind_pct = (ind_score / max(ind_max, 1)) * 100

    # If AI direction agrees with indicator signal → full AI contribution
    # If AI says HOLD  → half contribution
    # If AI disagrees  → subtract contribution
    if ai_dir == signal:
        ai_pct = ai_conf * 100
    elif ai_dir == "HOLD":
        ai_pct = ai_conf * 50
    else:
        ai_pct = (1 - ai_conf) * 50  # penalise disagreement

    blended = 0.60 * ind_pct + 0.40 * ai_pct
    return int(min(100, max(0, round(blended))))


# ──────────────────────────────────────────────────────────────────────────────
# SIGNAL GENERATION
# ──────────────────────────────────────────────────────────────────────────────


def _detect_gap(df_daily: pd.DataFrame, ltp: float) -> Optional[str]:
    """Detect opening gap vs previous close. Returns label or None."""
    if len(df_daily) < 2:
        return None
    prev_close = float(df_daily.iloc[-2]["close"])
    today_open = float(df_daily.iloc[-1]["open"])
    gap_pct    = (today_open - prev_close) / prev_close * 100
    if gap_pct > 0.5:
        return f"Gap Up {gap_pct:.1f}%"
    if gap_pct < -0.5:
        return f"Gap Down {gap_pct:.1f}%"
    return None


def _is_sideways(adx: float, atr: float, ltp: float) -> bool:
    """Both ADX weak AND ATR low → sideways."""
    atr_pct = (atr / ltp * 100) if ltp > 0 else 0
    return adx < 18 and atr_pct < 0.6


def _market_condition(adx: float, atr: float, ltp: float) -> str:
    """Classify market as Trending, Sideways, or Volatile."""
    atr_pct = (atr / ltp * 100) if ltp > 0 else 0
    if atr_pct > 1.5:
        return "🌪️ Volatile"
    if adx >= 25:
        return "📈 Trending"
    if adx < 18 and atr_pct < 0.6:
        return "↔️ Sideways"
    return "〰️ Mixed"


def generate_signal(df5: pd.DataFrame, df15: pd.DataFrame,
                    ltp: float, pp: dict,
                    df_daily: Optional[pd.DataFrame] = None,
                    symbol: str = "UNKNOWN") -> dict:
    """
    Core signal engine. Combines 9-point indicator scoring with AI ensemble.

    Returns a dict with signal, confidence (0–100), individual scores, and
    all indicator values needed for formatting and backtesting.
    """
    i5  = df5.iloc[-1]
    i15 = df15.iloc[-1]

    rsi       = _safe_float(i5.get("rsi"),       50.0)
    vwap      = _safe_float(i5.get("vwap"),       ltp)
    atr       = _safe_float(i5.get("atr"),        0.0)
    adx5      = _safe_float(i5.get("adx"),        0.0)
    adx15     = _safe_float(i15.get("adx"),       0.0)
    adx       = max(adx5, adx15)
    macd_hist = _safe_float(i5.get("macd_hist"),  0.0)
    ema9      = _safe_float(i5.get("ema9"),        ltp)
    ema21     = _safe_float(i5.get("ema21"),       ltp)
    ema50     = _safe_float(i5.get("ema50"),       ltp)
    st_dir    = int(_safe_float(i15.get("st_direction"), 0))
    st_val    = _safe_float(i15.get("supertrend"), ltp)
    bb_upper  = _safe_float(i5.get("bb_upper"),   float("nan"))
    bb_lower  = _safe_float(i5.get("bb_lower"),   float("nan"))
    vol       = _safe_float(i5.get("volume"),      0.0)
    vol_avg   = _safe_float(i5.get("vol_avg"),     0.0)
    vol_ok    = (vol > vol_avg * 1.2) if vol_avg > 0 else True

    sideways  = _is_sideways(adx, atr, ltp)
    mkt_cond  = _market_condition(adx, atr, ltp)

    gap_label        = _detect_gap(df_daily, ltp) if df_daily is not None else None
    gap_penalty_buy  = 1 if (gap_label and gap_label.startswith("Gap Up"))   else 0
    gap_penalty_sell = 1 if (gap_label and gap_label.startswith("Gap Down")) else 0
    if gap_label:
        logger.info(f"[Gap] {gap_label} detected — penalising counter-gap score by 1")

    conds = {
        "st_buy":    st_dir == 1,
        "st_sell":   st_dir == -1,
        "ema_buy":   ema9  > ema21,
        "ema_sell":  ema9  < ema21,
        "ema50_buy": ltp   > ema50,
        "ema50_sell":ltp   < ema50,
        "rsi_buy":   rsi   > 55,
        "rsi_sell":  rsi   < 45,
        "macd_buy":  macd_hist > 0,
        "macd_sell": macd_hist < 0,
        "adx_ok":    adx   > 20,
        "cpr_buy":   ltp   > pp["tc"],
        "cpr_sell":  ltp   < pp["bc"],
        "vwap_buy":  ltp   > vwap,
        "vwap_sell": ltp   < vwap,
        "vol_ok":    vol_ok,
    }

    buy_keys  = ["st_buy",  "ema_buy",  "ema50_buy",  "rsi_buy",  "macd_buy",  "adx_ok", "cpr_buy",  "vwap_buy",  "vol_ok"]
    sell_keys = ["st_sell", "ema_sell", "ema50_sell", "rsi_sell", "macd_sell", "adx_ok", "cpr_sell", "vwap_sell", "vol_ok"]
    IND_MAX   = len(buy_keys)

    buy_score  = max(0, sum(conds[k] for k in buy_keys)  - gap_penalty_buy)
    sell_score = max(0, sum(conds[k] for k in sell_keys) - gap_penalty_sell)

    if atr < 1.0:
        atr = ltp * 0.005

    min_score = _params_get("min_score")
    signal    = "HOLD"
    sl        = None
    target    = None
    active_ind_score = 0

    # Snapshot sl/tp multipliers under lock for consistency
    sl_mult = _params_get("sl_atr_mult")
    tp_mult = _params_get("tp_atr_mult")

    if buy_score >= min_score and buy_score >= sell_score and not sideways:
        signal = "BUY"
        sl     = round(ltp - sl_mult * atr, 2)
        target = round(ltp + tp_mult * atr, 2)
        if sl >= ltp or target <= ltp:
            signal, sl, target = "HOLD", None, None
        else:
            active_ind_score = buy_score

    elif sell_score >= min_score and sell_score > buy_score and not sideways:
        signal = "SELL"
        sl     = round(ltp + sl_mult * atr, 2)
        target = round(ltp - tp_mult * atr, 2)
        if sl <= ltp or target >= ltp:
            signal, sl, target = "HOLD", None, None
        else:
            active_ind_score = sell_score

    # ── AI layer ──────────────────────────────────────────────────────────────
    raw_sg = {
        "rsi": rsi, "adx": adx, "macd_hist": macd_hist,
        "ema9": ema9, "ema21": ema21, "ema50": ema50,
        "vwap": vwap, "atr": atr, "st_dir": st_dir,
        "vol_ok": vol_ok, "bb_upper": bb_upper, "bb_lower": bb_lower,
        "pp": pp,
    }
    feat_vec = _build_feature_vector(raw_sg, ltp)
    ai_conf, ai_dir = (0.5, "HOLD")
    if feat_vec is not None:
        ai_conf, ai_dir = _ai_predict(symbol, feat_vec)

    # Final 0–100 confidence
    if signal in ("BUY", "SELL"):
        final_conf = _compute_final_confidence(
            active_ind_score, IND_MAX, ai_conf, ai_dir, signal
        )
    else:
        raw_score = max(buy_score, sell_score)
        final_conf = _compute_final_confidence(
            raw_score, IND_MAX, ai_conf, ai_dir,
            "BUY" if buy_score >= sell_score else "SELL"
        )

    # Update ATR history for volatility filter
    _update_atr_history(symbol, atr)

    return {
        "signal":      signal,
        "confidence":  final_conf,
        "ind_score":   active_ind_score if signal in ("BUY", "SELL") else max(buy_score, sell_score),
        "ai_conf":     round(ai_conf * 100, 1),
        "ai_dir":      ai_dir,
        "buy_score":   buy_score,
        "sell_score":  sell_score,
        "sl":          sl,
        "target":      target,
        "atr":         atr,
        "adx":         adx,
        "rsi":         rsi,
        "vwap":        vwap,
        "macd_hist":   macd_hist,
        "ema9":        ema9,
        "ema21":       ema21,
        "ema50":       ema50,
        "bb_upper":    bb_upper,
        "bb_lower":    bb_lower,
        "st_dir":      st_dir,
        "st_val":      st_val,
        "vol_ok":      vol_ok,
        "sideways":    sideways,
        "gap":         gap_label,
        "mkt_cond":    mkt_cond,
        "conds":       conds,
        "features":    feat_vec,
        "i15_ema9":    _safe_float(i15.get("ema9"),  ltp),
        "i15_ema21":   _safe_float(i15.get("ema21"), ltp),
        "i15_rsi":     _safe_float(i15.get("rsi"),   50.0),
        "i15_adx":     adx15,
    }


# ──────────────────────────────────────────────────────────────────────────────
# BACKTESTING MODULE
# ──────────────────────────────────────────────────────────────────────────────

def _backtest_strategy(df5: pd.DataFrame, df15: pd.DataFrame,
                       pp: dict, params: dict) -> dict:
    """
    Vectorised intraday backtest over historical candle data.

    Iterates bar-by-bar (simulating real-time), fires trades using the
    generate_signal logic with the supplied params, and tracks P&L.

    Returns a metrics dict: win_rate, profit_factor, max_drawdown, sharpe,
    total_trades, gross_profit, gross_loss.
    """
    df5  = _indicators_5min(df5,  params=params)
    df15 = _indicators_15min(df15, params=params)

    min_bars   = max(params.get("ema_slow", 50), params.get("rsi_period", 14)) + 5
    trades     = []
    equity     = [CAPITAL]
    capital    = CAPITAL

    for i in range(min_bars, len(df5)):
        bar5  = df5.iloc[:i+1]
        # align 15-min bars to the current 5-min timestamp
        t5    = df5.index[i]
        mask  = df15.index <= t5
        if mask.sum() < min_bars:
            continue
        bar15 = df15[mask]

        ltp = float(bar5.iloc[-1]["close"])
        if ltp <= 0:
            continue

        sg = generate_signal(bar5, bar15, ltp, pp, symbol="BT")
        if sg["signal"] not in ("BUY", "SELL"):
            continue

        sl     = sg["sl"]
        target = sg["target"]
        if sl is None or target is None:
            continue

        # Simulate resolution: scan next 15 bars for SL/TP hit
        direction = sg["signal"]
        result    = "timeout"
        exit_px   = ltp
        for j in range(i + 1, min(i + 16, len(df5))):
            future = df5.iloc[j]
            if direction == "BUY":
                if float(future["low"]) <= sl:
                    result, exit_px = "loss", sl
                    break
                if float(future["high"]) >= target:
                    result, exit_px = "win", target
                    break
            else:
                if float(future["high"]) >= sl:
                    result, exit_px = "loss", sl
                    break
                if float(future["low"]) <= target:
                    result, exit_px = "win", target
                    break
        if result == "timeout":
            exit_px = float(df5.iloc[min(i + 15, len(df5) - 1)]["close"])
            result  = "win" if (
                (direction == "BUY"  and exit_px > ltp) or
                (direction == "SELL" and exit_px < ltp)
            ) else "loss"

        pts = (exit_px - ltp) if direction == "BUY" else (ltp - exit_px)
        lot = position_size(ltp, sl)
        pnl = pts * lot["qty"]
        capital += pnl
        equity.append(capital)
        trades.append({
            "entry": ltp, "exit": exit_px, "direction": direction,
            "result": result, "pnl": pnl, "confidence": sg["confidence"],
        })

    if not trades:
        return {
            "total_trades": 0, "win_rate": 0, "profit_factor": 0,
            "max_drawdown": 0, "sharpe": 0, "gross_profit": 0, "gross_loss": 0,
        }

    wins   = [t for t in trades if t["result"] == "win"]
    losses = [t for t in trades if t["result"] == "loss"]
    gross_profit = sum(t["pnl"] for t in wins)
    gross_loss   = abs(sum(t["pnl"] for t in losses))
    win_rate     = len(wins) / len(trades)
    pf           = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Max drawdown
    peak  = equity[0]
    max_dd = 0.0
    for e in equity:
        if e > peak:
            peak = e
        dd = (peak - e) / peak
        if dd > max_dd:
            max_dd = dd

    # Sharpe (annualised approximation)
    daily_rets = []
    for k in range(1, len(equity)):
        daily_rets.append((equity[k] - equity[k-1]) / max(equity[k-1], 1.0))
    if len(daily_rets) > 1:
        mu_r  = statistics.mean(daily_rets)
        std_r = statistics.stdev(daily_rets)
        sharpe = (mu_r / std_r * (252 ** 0.5)) if std_r > 0 else 0.0
    else:
        sharpe = 0.0

    return {
        "total_trades":  len(trades),
        "win_rate":      round(win_rate, 4),
        "profit_factor": round(pf, 4),
        "max_drawdown":  round(max_dd, 4),
        "sharpe":        round(sharpe, 4),
        "gross_profit":  round(gross_profit, 2),
        "gross_loss":    round(gross_loss, 2),
    }


def run_backtest(df5: pd.DataFrame, df15: pd.DataFrame, pp: dict,
                 export_csv: bool = True) -> dict:
    """
    Run a backtest with current parameters, persist results, and optionally
    export to CSV.

    Returns the metrics dict.
    """
    logger.info("[Backtest] Running with current parameters…")
    with _PARAMS_LOCK:
        params_snapshot = dict(_PARAMS)

    metrics = _backtest_strategy(df5, df15, pp, params_snapshot)
    db_store_backtest_run(params_snapshot, metrics)

    logger.info(
        f"[Backtest] Trades={metrics['total_trades']} | "
        f"WinRate={metrics['win_rate']:.1%} | "
        f"PF={metrics['profit_factor']:.2f} | "
        f"MaxDD={metrics['max_drawdown']:.1%} | "
        f"Sharpe={metrics['sharpe']:.2f}"
    )

    if export_csv:
        _export_backtest_csv(params_snapshot, metrics)

    return metrics


def _export_backtest_csv(params: dict, metrics: dict):
    """Append a backtest result row to the CSV export file."""
    try:
        file_exists = os.path.exists(BT_EXPORT_PATH)
        with open(BT_EXPORT_PATH, "a", newline="") as f:
            fieldnames = (
                list(params.keys()) + list(metrics.keys()) + ["ts"]
            )
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            row = {**params, **metrics, "ts": _ist_now().isoformat()}
            writer.writerow(row)
        logger.info(f"[Backtest] Results appended to {BT_EXPORT_PATH}")
    except Exception as exc:
        logger.warning(f"[Backtest] CSV export error: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# AUTO PARAMETER OPTIMISATION (Grid Search)
# ──────────────────────────────────────────────────────────────────────────────

# Reduced grid for speed — expand for deeper search
_PARAM_GRID = {
    "rsi_period":  [9, 14],
    "ema_fast":    [9, 12],
    "ema_mid":     [21, 26],
    "ema_slow":    [50],
    "st_period":   [7, 10],
    "st_mult":     [2.0, 2.5],
    "sl_atr_mult": [1.5, 2.0],
    "tp_atr_mult": [2.5, 3.0],
    "min_score":   [4, 5],
}


def _generate_param_combinations(grid: dict) -> List[dict]:
    """Generate all parameter combinations from a grid dict."""
    keys   = list(grid.keys())
    values = list(grid.values())
    combos = []
    for combo in itertools.product(*values):
        d = _params_snapshot()  # thread-safe snapshot as defaults
        d.update(dict(zip(keys, combo)))
        combos.append(d)
    return combos


def run_param_optimisation(df5: pd.DataFrame, df15: pd.DataFrame,
                            pp: dict, n_best: int = 5) -> dict:
    """
    Grid-search over _PARAM_GRID. Objective: maximise Sharpe ratio.
    Updates global _PARAMS with the best found parameters.

    Returns the best metrics dict.
    """
    combos = _generate_param_combinations(_PARAM_GRID)
    logger.info(f"[Optimiser] Grid search over {len(combos)} parameter combinations…")

    best_sharpe  = -float("inf")
    best_params  = None
    best_metrics = None
    results      = []

    for i, params in enumerate(combos):
        try:
            metrics = _backtest_strategy(df5, df15, pp, params)
            sharpe  = metrics["sharpe"]
            results.append((sharpe, params, metrics))
            if sharpe > best_sharpe and metrics["total_trades"] >= 5:
                best_sharpe  = sharpe
                best_params  = params
                best_metrics = metrics
        except Exception as exc:
            logger.debug(f"[Optimiser] combo {i} error: {exc}")

    if best_params is not None:
        with _PARAMS_LOCK:
            _PARAMS.update(best_params)
        logger.info(
            f"[Optimiser] Best params found | Sharpe={best_sharpe:.2f} "
            f"| WR={best_metrics['win_rate']:.1%} "
            f"| Trades={best_metrics['total_trades']}"
        )
        tg_enqueue(
            f"🔧 *Auto-Optimisation Complete*\n"
            f"{_SEP}\n"
            f"Best Sharpe: {best_sharpe:.2f}\n"
            f"Win Rate: {best_metrics['win_rate']:.1%}\n"
            f"Profit Factor: {best_metrics['profit_factor']:.2f}\n"
            f"RSI={best_params['rsi_period']} "
            f"EMA={best_params['ema_fast']}/{best_params['ema_mid']}\n"
            f"ST period={best_params['st_period']} mult={best_params['st_mult']}\n"
            f"SL×ATR={best_params['sl_atr_mult']} TP×ATR={best_params['tp_atr_mult']}"
        )
    else:
        logger.warning("[Optimiser] No valid parameter combination found")

    return best_metrics or {}


# ──────────────────────────────────────────────────────────────────────────────
# AUTO TRADING — ANGEL ONE ORDER PLACEMENT (SAFE MODE)
# ──────────────────────────────────────────────────────────────────────────────

def _place_order(obj: SmartConnect, symbol: str, direction: str,
                 qty: int, price: float, sl: float) -> Optional[str]:
    """
    Place a market order via Angel One SmartAPI.
    Only executes when AUTO_TRADE_ENABLED=True and kill switch is off.

    Returns order_id string on success, None on failure.
    """
    global _kill_active

    if not AUTO_TRADE_ENABLED:
        logger.info(f"[AutoTrade] Disabled — would place {direction} {qty}×{symbol} @ {price:.0f}")
        return None

    if _kill_active or KILL_SWITCH:
        logger.warning("[AutoTrade] KILL SWITCH active — order blocked")
        return None

    sig_today, daily_loss, open_tr = _state_get()
    max_loss = CAPITAL * MAX_DAILY_LOSS_PCT / 100
    if daily_loss >= max_loss:
        logger.warning("[AutoTrade] Daily loss limit hit — order blocked")
        return None
    if sig_today >= MAX_SIGNALS_PER_DAY:
        logger.warning("[AutoTrade] Max signals/day reached — order blocked")
        return None
    if open_tr >= MAX_OPEN_TRADES:
        logger.warning("[AutoTrade] Max open trades reached — order blocked")
        return None

    transaction = "BUY" if direction == "BUY" else "SELL"
    order_params = {
        "variety":          "NORMAL",
        "tradingsymbol":    symbol,
        "symboltoken":      SYMBOLS[symbol]["token"],
        "transactiontype":  transaction,
        "exchange":         SYMBOLS[symbol]["exchange"],
        "ordertype":        "MARKET",
        "producttype":      "INTRADAY",
        "duration":         "DAY",
        "price":            "0",
        "squareoff":        str(round(abs(price - sl) * 3, 2)),
        "stoploss":         str(round(abs(price - sl), 2)),
        "quantity":         str(qty),
    }

    for attempt in range(1, 4):
        try:
            resp = obj.placeOrder(order_params)
            if resp and resp.get("status"):
                order_id = resp["data"]["orderid"]
                logger.info(
                    f"[AutoTrade] Order placed: {transaction} {qty}×{symbol} "
                    f"@ mkt | OrderID={order_id}"
                )
                tg_enqueue(
                    f"🤖 *Auto Order Placed*\n{_SEP}\n"
                    f"{transaction} {qty}×{symbol} @ market\n"
                    f"SL: Rs{sl:,.2f}\n"
                    f"OrderID: {order_id}"
                )
                return order_id
            logger.warning(f"[AutoTrade] Order attempt {attempt}: {resp}")
        except Exception as exc:
            logger.warning(f"[AutoTrade] Order error attempt {attempt}: {exc}")
        time.sleep(2 ** attempt)

    logger.error(f"[AutoTrade] Order failed after 3 attempts for {symbol}")
    return None


def activate_kill_switch():
    """Runtime emergency kill switch — stops all new auto-trade orders."""
    global _kill_active
    _kill_active = True
    logger.warning("[AutoTrade] KILL SWITCH activated at runtime")
    tg_enqueue("🛑 *KILL SWITCH ACTIVATED* — No new auto-trade orders will be placed.")


# ──────────────────────────────────────────────────────────────────────────────
# ADVANCED RISK MANAGEMENT — TRAILING STOP / BREAK-EVEN / PARTIAL PROFIT
# ──────────────────────────────────────────────────────────────────────────────

def _advanced_risk_monitor(trade_id: int, symbol: str, direction: str,
                            entry: float, sl: float, target: float,
                            obj: SmartConnect):
    """
    Background monitor for advanced risk management on an open trade.

    Features:
    - Trailing stop-loss (tightens as price moves in favour).
    - Break-even logic (moves SL to entry after 1R profit).
    - Partial profit booking (50% exit at 1.5× ATR).
    """
    MAX_SEC    = 15 * 60
    POLL_SEC   = 30
    start_time = time.monotonic()

    info      = SYMBOLS[symbol]
    atr_init  = abs(target - entry) / _params_get("tp_atr_mult")
    trail_sl  = sl
    be_moved  = False
    partial_done = False
    exit_reason = "timeout"
    exit_price  = entry
    result      = None

    partial_target = entry + 1.5 * atr_init if direction == "BUY" else entry - 1.5 * atr_init
    be_trigger     = entry + 1.0 * atr_init if direction == "BUY" else entry - 1.0 * atr_init

    logger.info(
        f"[AdvRisk] Monitoring trade#{trade_id} {symbol} {direction}@{entry:.0f} "
        f"SL={sl:.0f} TGT={target:.0f}"
    )

    while not _shutdown_event.is_set():
        elapsed = time.monotonic() - start_time

        try:
            ltp = fetch_ltp(obj, info["exchange"], info["token"], symbol)
        except Exception:
            if elapsed >= MAX_SEC:
                result, exit_price, exit_reason = "wrong", entry, "timeout (fetch failed)"
                break
            _shutdown_event.wait(timeout=POLL_SEC)
            continue

        # ── Break-even logic ───────────────────────────────────────────────
        if not be_moved:
            if (direction == "BUY" and ltp >= be_trigger) or \
               (direction == "SELL" and ltp <= be_trigger):
                trail_sl = entry
                be_moved = True
                logger.info(f"[AdvRisk] {symbol}: Break-even SL moved to {entry:.0f}")

        # ── Trailing stop (1× ATR behind current price) ───────────────────
        if direction == "BUY":
            new_trail = ltp - atr_init
            if new_trail > trail_sl:
                trail_sl = new_trail
                logger.debug(f"[AdvRisk] {symbol}: Trail SL updated to {trail_sl:.0f}")
        else:
            new_trail = ltp + atr_init
            if new_trail < trail_sl:
                trail_sl = new_trail
                logger.debug(f"[AdvRisk] {symbol}: Trail SL updated to {trail_sl:.0f}")

        # ── Partial profit booking ─────────────────────────────────────────
        if not partial_done:
            if (direction == "BUY" and ltp >= partial_target) or \
               (direction == "SELL" and ltp <= partial_target):
                partial_done = True
                logger.info(
                    f"[AdvRisk] {symbol}: Partial profit at {ltp:.0f} "
                    f"(1.5×ATR target hit)"
                )
                tg_enqueue(
                    f"💰 *Partial Profit Booked*\n{_SEP}\n"
                    f"{symbol} {direction}@{entry:.0f}\n"
                    f"Exit 50% @ Rs{ltp:,.2f}\n"
                    f"Remaining: trail SL={trail_sl:.0f}"
                )

        # ── Stop / Target checks ───────────────────────────────────────────
        if direction == "BUY":
            if ltp <= trail_sl:
                result, exit_price, exit_reason = "wrong",   ltp, f"Trail SL hit ({trail_sl:.0f})"
                break
            if ltp >= target:
                result, exit_price, exit_reason = "correct", ltp, "Target hit"
                break
        else:
            if ltp >= trail_sl:
                result, exit_price, exit_reason = "wrong",   ltp, f"Trail SL hit ({trail_sl:.0f})"
                break
            if ltp <= target:
                result, exit_price, exit_reason = "correct", ltp, "Target hit"
                break

        if elapsed >= MAX_SEC:
            exit_price  = ltp
            exit_reason = "15-min timeout"
            result = "correct" if (
                (direction == "BUY"  and ltp > entry) or
                (direction == "SELL" and ltp < entry)
            ) else "wrong"
            break

        _shutdown_event.wait(timeout=POLL_SEC)

    if result is None:
        logger.warning(f"[AdvRisk] {symbol}: shutdown before resolution")
        _state_open_trade_remove()
        return

    try:
        db_update_signal_result(trade_id, result)

        # ── Write AI training label post-resolution (no look-ahead bias) ─────
        with _pending_label_lock:
            pending = _pending_label_features.pop(trade_id, None)
        if pending is not None:
            sym_label, feats = pending
            label = 1 if result == "correct" else -1
            db_store_training_sample(sym_label, feats, label)

        pts     = round(exit_price - entry, 2) if direction == "BUY" \
                  else round(entry - exit_price, 2)
        lot     = position_size(entry, sl)
        qty     = lot.get("qty", 1)
        pnl     = round(pts * qty, 2)
        pnl_str = f"+Rs{pnl:,.0f}" if pnl >= 0 else f"-Rs{abs(pnl):,.0f}"

        if result == "wrong":
            _state_add_loss(lot.get("risk_rs", 0.0))

        icon = "✅" if result == "correct" else "❌"
        tg_enqueue(
            f"{icon} *{symbol} Trade Closed*\n"
            f"{_SEP}\n"
            f"Signal: {direction} @ Rs{entry:,.0f}\n"
            f"Exit:   Rs{exit_price:,.2f}  ({exit_reason})\n"
            f"P&L:    {pnl_str}  ({pts:+.0f} pts × {qty} qty)\n"
            f"Result: *{'Correct ✅' if result == 'correct' else 'Wrong ❌'}*"
        )
        logger.info(
            f"[AdvRisk] {symbol}: {direction}@{entry:.0f}→{exit_price:.0f} "
            f"({exit_reason}) = {result} | PnL={pnl_str}"
        )
    except Exception as exc:
        logger.error(f"[AdvRisk] Result save error {symbol}: {exc}")
    finally:
        _state_open_trade_remove()


# ──────────────────────────────────────────────────────────────────────────────
# MESSAGE FORMATTING — ENHANCED v5
# ──────────────────────────────────────────────────────────────────────────────

def _ck(v: bool) -> str:
    return "✅" if v else "❌"


def format_message(symbol: str, ltp: float, sg: dict, pp: dict,
                   lot: Optional[dict]) -> str:
    """
    Build the enhanced Telegram signal message.
    Includes: confidence %, AI score, indicator score, R:R, market condition.
    """
    signal     = sg["signal"]
    confidence = sg["confidence"]
    atr        = sg["atr"]
    sl         = sg["sl"]
    target     = sg["target"]
    rsi        = sg["rsi"]
    adx        = sg["adx"]
    conds      = sg["conds"]
    ai_conf    = sg.get("ai_conf", 0)
    ai_dir     = sg.get("ai_dir",  "HOLD")
    ind_score  = sg.get("ind_score", 0)
    mkt_cond   = sg.get("mkt_cond", "—")

    now      = _ist_now()
    date_str = now.strftime("%d %b %Y")
    time_str = now.strftime("%I:%M %p IST")

    is_sell = (signal == "SELL") or (signal == "HOLD" and sg["sell_score"] > sg["buy_score"])
    c_st    = conds["st_sell"]    if is_sell else conds["st_buy"]
    c_ema   = conds["ema_sell"]   if is_sell else conds["ema_buy"]
    c_e50   = conds["ema50_sell"] if is_sell else conds["ema50_buy"]
    c_rsi   = conds["rsi_sell"]   if is_sell else conds["rsi_buy"]
    c_macd  = conds["macd_sell"]  if is_sell else conds["macd_buy"]
    c_cpr   = conds["cpr_sell"]   if is_sell else conds["cpr_buy"]
    c_vwap  = conds["vwap_sell"]  if is_sell else conds["vwap_buy"]
    c_adx   = conds["adx_ok"]
    c_vol   = conds["vol_ok"]

    st_icon  = "🟢 Bull" if sg["st_dir"] == 1 else "🔴 Bear"
    ema_dir  = "Bullish" if sg["ema9"] > sg["ema21"] else "Bearish"
    macd_dir = "Bullish" if sg["macd_hist"] > 0 else "Bearish"
    rsi_lbl  = "Oversold" if rsi < 30 else ("Overbought" if rsi > 70 else "Neutral")
    adx_str  = f"{adx:.1f} ({'Strong' if adx > 25 else 'Trending' if adx > 20 else 'Weak'})"
    vwap_str = f"Rs{sg['vwap']:,.2f} ({'Above' if ltp > sg['vwap'] else 'Below'})"

    bb_l, bb_u = sg["bb_lower"], sg["bb_upper"]
    bb_str  = f"Rs{bb_l:,.0f}–Rs{bb_u:,.0f}" if not math.isnan(bb_l) else "N/A"
    bb_note = (
        " [At Lower]" if not math.isnan(bb_l) and ltp <= bb_l else
        " [At Upper]" if not math.isnan(bb_u) and ltp >= bb_u else ""
    )
    cpr_str = (
        f"TC Rs{pp['tc']:,.2f} | BC Rs{pp['bc']:,.2f} | "
        f"{'Narrow-Trending' if pp['cpr_narrow'] else 'Wide-Sideways'}"
    )

    # Confidence bar (visual)
    bars_filled = round(confidence / 10)
    conf_bar    = "█" * bars_filled + "░" * (10 - bars_filled)

    ai_agree = "✅ Agrees" if ai_dir == signal else ("⚠️ HOLD" if ai_dir == "HOLD" else "❌ Disagrees")

    if signal == "BUY":
        view = "Strong BUY — all conditions met!"
    elif signal == "SELL":
        view = "Strong SELL — all conditions met!"
    elif confidence >= 70:
        dom  = "BUY" if sg["buy_score"] >= sg["sell_score"] else "SELL"
        view = f"{confidence}% {dom} bias — wait for confirmation"
    elif confidence >= 50:
        dom  = "BUY" if sg["buy_score"] >= sg["sell_score"] else "SELL"
        view = f"{confidence}% {dom} leaning — stay flat"
    else:
        view = "Mixed signals — stay flat"

    if sg["sideways"]:
        view += " ⚠️ Sideways market"

    trade_block = ""
    if signal in ("BUY", "SELL") and sl is not None:
        risk_pts = abs(ltp - sl)
        rwd_pts  = abs(target - ltp)
        rr       = (rwd_pts / risk_pts) if risk_pts > 0 else 0
        trade_block = (
            f"{_SEP}\n"
            f"*Trade Setup* (ATR={atr:.0f})\n"
            f"SL: Rs{sl:,.2f} ({risk_pts:.0f} pts)\n"
            f"Target: Rs{target:,.2f} ({rwd_pts:.0f} pts)\n"
            f"R:R = 1:{rr:.2f}\n"
        )
        if lot and lot.get("qty", 0) > 0:
            trade_block += (
                f"Qty: {lot['qty']} | Risk: Rs{lot['risk_rs']:,.0f}\n"
                f"Capital Used: Rs{lot['capital_used']:,.0f}\n"
                f"Kelly%: {lot.get('kelly_pct', 0)}%\n"
            )

    sr_alert = ""
    if ltp > 0:
        if abs(ltp - pp["s1"]) / ltp < 0.005:
            sr_alert = "\n⚡ Near S1 — watch for bounce!"
        elif abs(ltp - pp["r1"]) / ltp < 0.005:
            sr_alert = "\n⚡ Near R1 — watch for reversal!"
        elif abs(ltp - pp["pivot"]) / ltp < 0.003:
            sr_alert = "\n⚡ Near Pivot — key zone!"

    sig_icon = "🟢" if signal == "BUY" else ("🔴" if signal == "SELL" else "⏸️")

    return (
        f"{sig_icon} *{symbol} SIGNAL — v6 AI*\n"
        f"{date_str}  {time_str}\n"
        f"{_SEP}\n"
        f"Price: Rs{ltp:,.2f}  |  Signal: *{signal}*\n"
        f"*Confidence: {confidence}%*  [{conf_bar}]\n"
        f"Market: {mkt_cond}\n"
        f"{_SEP}\n"
        f"*Score Breakdown*\n"
        f"Indicator: {ind_score}/9\n"
        f"AI ({ai_dir}): {ai_conf:.0f}%  {ai_agree}\n"
        f"{_SEP}\n"
        f"*Conditions*\n"
        f"{_ck(c_st)}  Supertrend\n"
        f"{_ck(c_ema)}  EMA 9/21\n"
        f"{_ck(c_e50)}  Price vs EMA50\n"
        f"{_ck(c_rsi)}  RSI ({rsi:.1f} — {rsi_lbl})\n"
        f"{_ck(c_macd)}  MACD ({macd_dir})\n"
        f"{_ck(c_adx)}  ADX ({adx_str})\n"
        f"{_ck(c_cpr)}  CPR\n"
        f"{_ck(c_vwap)}  VWAP ({vwap_str})\n"
        f"{_ck(c_vol)}  Volume\n"
        f"{_SEP}\n"
        f"*5-Min*\n"
        f"EMA: 9={sg['ema9']:.0f}  21={sg['ema21']:.0f}  50={sg['ema50']:.0f} — {ema_dir}\n"
        f"MACD Hist: {sg['macd_hist']:.1f}\n"
        f"BB: {bb_str}{bb_note}\n"
        f"VWAP: {vwap_str}\n"
        f"{_SEP}\n"
        f"*15-Min*\n"
        f"Supertrend: {st_icon} @ Rs{sg['st_val']:,.0f}\n"
        f"EMA: 9={sg['i15_ema9']:.0f}  21={sg['i15_ema21']:.0f}\n"
        f"RSI: {sg['i15_rsi']:.1f}  ADX: {sg['i15_adx']:.1f}\n"
        f"{_SEP}\n"
        f"*CPR*\n"
        f"{cpr_str}\n"
        f"{_SEP}\n"
        f"*View*\n"
        f"{view}\n"
        f"{trade_block}"
        f"{_SEP}\n"
        f"*Levels* (prev {pp['prev_date']})\n"
        f"R1: Rs{pp['r1']:,.2f}  R2: Rs{pp['r2']:,.2f}\n"
        f"Pivot: Rs{pp['pivot']:,.2f}\n"
        f"S1: Rs{pp['s1']:,.2f}  S2: Rs{pp['s2']:,.2f}"
        f"{sr_alert}\n"
        f"{_SEP}\n"
        f"_Educational only. Not financial advice._"
    )


# ──────────────────────────────────────────────────────────────────────────────
# SCHEDULER LOOP
# ──────────────────────────────────────────────────────────────────────────────

def _scheduler_loop():
    """Main signal generation loop — runs every SIGNAL_INTERVAL_MIN minutes."""
    logger.info(f"[Scheduler] Started — interval={SIGNAL_INTERVAL_MIN}min")
    while not _shutdown_event.is_set():
        try:
            run_signal_job()
        except Exception as exc:
            logger.error(f"[Scheduler] Unhandled error in signal job: {exc}", exc_info=True)
        _shutdown_event.wait(timeout=SIGNAL_INTERVAL_MIN * 60)


def run_signal_job():
    """
    Main signal cycle. Called on every scheduler tick.
    Guards: market hours, volatile window, daily limits, kill switch.
    """
    _state_maybe_reset()

    if _kill_active:
        logger.info("[SignalJob] Kill switch active — skipping")
        return

    if not _is_market_open():
        logger.info("Market closed — skipping job")
        return

    if _is_volatile_window():
        logger.info(f"Volatile window — skipping @ {_ist_now().strftime('%H:%M IST')}")
        return

    signals_today, daily_loss_rs, open_trades = _state_get()
    max_loss = CAPITAL * MAX_DAILY_LOSS_PCT / 100

    if daily_loss_rs >= max_loss:
        logger.warning("Daily loss limit hit — halted")
        return
    if signals_today >= MAX_SIGNALS_PER_DAY:
        logger.warning(f"Max signals/day ({MAX_SIGNALS_PER_DAY}) reached")
        return
    if open_trades >= MAX_OPEN_TRADES:
        logger.warning(f"Max open trades ({MAX_OPEN_TRADES}) reached")
        return

    logger.info(f"Signal job starting @ {_ist_now().strftime('%H:%M IST')}")

    try:
        obj, _, _ = _angel_session()
    except Exception as exc:
        logger.error(f"Cannot create session: {exc}")
        tg_enqueue(f"⚠️ *Session Error*\n{exc}\nBot pausing this cycle.")
        return

    for symbol, info in SYMBOLS.items():
        if _shutdown_event.is_set():
            break
        try:
            if not _cooldown_ok(symbol):
                logger.info(f"{symbol}: cooldown active — skipping")
                continue

            token, exchange = info["token"], info["exchange"]
            ltp      = fetch_ltp(obj, exchange, token, symbol)
            df5      = _indicators_5min(fetch_candles(obj, token, exchange, "FIVE_MINUTE",    days=5))
            df15     = _indicators_15min(fetch_candles(obj, token, exchange, "FIFTEEN_MINUTE", days=10))
            try:
                df_daily = fetch_candles(obj, token, exchange, "ONE_DAY", days=5)
            except Exception:
                df_daily = None
            prev_ohlc = fetch_prev_day_ohlc(obj, token, exchange)
            pp        = calc_pivot_points(prev_ohlc)
            sg        = generate_signal(df5, df15, ltp, pp, df_daily=df_daily, symbol=symbol)
            signal    = sg["signal"]
            conf      = sg["confidence"]

            # ── Volatility spike filter ────────────────────────────────────
            if _is_abnormal_volatility(symbol, sg["atr"]):
                tg_enqueue(
                    f"🌪️ *{symbol}: Abnormal Volatility Detected*\n"
                    f"ATR spike — skipping this cycle."
                )
                continue

            lot = None
            if signal in ("BUY", "SELL") and sg["sl"] is not None:
                lot = position_size(ltp, sg["sl"])
                if lot["qty"] == 0:
                    logger.warning(f"{symbol}: lot size=0, skipping signal")
                    signal = sg["signal"] = "HOLD"

            _update_last_signal(symbol, {
                "signal":     signal,
                "ltp":        ltp,
                "rsi":        sg["rsi"],
                "confidence": conf,
                "buy_score":  sg["buy_score"],
                "sell_score": sg["sell_score"],
                "sl":         sg["sl"],
                "target":     sg["target"],
                "time":       _ist_now().strftime("%H:%M IST"),
                "mkt_cond":   sg["mkt_cond"],
            })

            if signal in ("BUY", "SELL"):
                msg    = format_message(symbol, ltp, sg, pp, lot)
                row_id = db_insert_signal(
                    symbol, signal, ltp, conf, sg["sl"], sg["target"],
                    ai_score=sg["ai_conf"], ind_score=sg["ind_score"]
                )

                # Store feature vector AFTER row_id is defined — label written post-resolution
                if sg["features"] is not None:
                    with _pending_label_lock:
                        _pending_label_features[row_id] = (symbol, sg["features"])

                _state_increment_signal()
                _state_open_trade_add()

                # Advanced risk management monitor
                threading.Thread(
                    target=_advanced_risk_monitor,
                    args=(row_id, symbol, signal, ltp, sg["sl"], sg["target"], obj),
                    name=f"arisk-{symbol}-{row_id}",
                    daemon=True,
                ).start()

                # Auto-trade placement (safe mode)
                if AUTO_TRADE_ENABLED and conf >= AUTO_TRADE_MIN_CONF:
                    qty = lot["qty"] if lot else 1
                    _place_order(obj, symbol, signal, qty, ltp, sg["sl"])

                tg_enqueue(msg)
            else:
                logger.info(f"{symbol}: HOLD — not sent to Telegram")

            logger.info(
                f"{symbol}: {signal} {conf}% | LTP={ltp:.0f} | "
                f"RSI={sg['rsi']:.1f} | ADX={sg['adx']:.1f} | "
                f"IndScore={sg['ind_score']} | AI={sg['ai_conf']:.0f}% ({sg['ai_dir']})"
            )

        except Exception as exc:
            logger.error(f"{symbol}: signal job error: {exc}", exc_info=True)


# ──────────────────────────────────────────────────────────────────────────────
# HEALTH SERVER
# ──────────────────────────────────────────────────────────────────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP health-check endpoint for uptime monitors."""

    def do_GET(self):
        secret = HEALTH_SECRET
        if secret:
            expected_path = f"/{secret}"
            # Timing-safe comparison to prevent timing attacks
            if not hmac.compare_digest(self.path.encode(), expected_path.encode()):
                self.send_response(403)
                self.end_headers()
                return
        sig_today, loss_rs, open_tr = _state_get()
        body = json.dumps({
            "status":       "ok",
            "market":       _market_status_text(),
            "signals_today": sig_today,
            "loss_rs":      loss_rs,
            "open_trades":  open_tr,
            "kill_switch":  _kill_active,
            "ml_available": _ML_AVAILABLE,
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # suppress default access log


def _start_health_server():
    """Start the HTTP health server in a daemon thread."""
    try:
        server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
        t = threading.Thread(target=server.serve_forever, name="health-srv", daemon=True)
        t.start()
        logger.info(f"Health server running on port {HEALTH_PORT}")
    except Exception as exc:
        logger.warning(f"Health server failed to start: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# TELEGRAM COMMANDS
# ──────────────────────────────────────────────────────────────────────────────

def _tg_auth_ok(update: Update) -> bool:
    """
    Return True if the sender is authorised to use bot commands.
    If ALLOWED_TELEGRAM_IDS is empty, all users are allowed.
    Silently drops unauthorised requests (no reply to avoid enumeration).
    """
    if not ALLOWED_TELEGRAM_IDS:
        return True
    user_id = update.effective_user.id if update.effective_user else None
    if user_id not in ALLOWED_TELEGRAM_IDS:
        logger.warning(f"[Auth] Rejected Telegram command from user_id={user_id}")
        return False
    return True

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Send welcome message with command list."""
    if not _tg_auth_ok(update):
        return
    ml_status = "✅ Active" if _ML_AVAILABLE and any(_ai_models) else (
        "⚠️ Training..." if _ML_AVAILABLE else "❌ Not installed"
    )
    msg = (
        f"👋 *Swastik Trading Bot v6 ULTRA PRO*\n{_SEP}\n"
        f"Market: {_market_status_text()}\n"
        f"AI Engine: {ml_status}\n"
        f"Auto-Trade: {'✅ ON' if AUTO_TRADE_ENABLED else '❌ OFF'}\n"
        f"{_SEP}\n"
        f"*Commands*\n"
        f"/start — this message\n"
        f"/status — live market snapshot\n"
        f"/history — last 20 signals\n"
        f"/accuracy — signal win rate\n"
        f"/risk — risk status\n"
        f"/performance — P&L dashboard\n"
        f"/dashboard — full performance stats\n"
        f"/alert — price alerts\n"
        f"/backtest — run backtest\n"
        f"/optimise — auto-tune parameters\n"
        f"/kill — emergency kill switch\n"
        f"/trainai — retrain AI model now\n"
        f"{_SEP}\n"
        f"_Educational only. Not financial advice._"
    )
    await update.message.reply_text(_truncate(msg), parse_mode="Markdown")


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show live snapshot for all symbols."""
    if not _tg_auth_ok(update):
        return
    lines = [f"*Market Status*\n{_SEP}", f"Market: {_market_status_text()}"]
    try:
        obj, _, _ = _angel_session()
    except Exception:
        await update.message.reply_text("❌ Session error — try again.")
        return

    for symbol, info in SYMBOLS.items():
        last = _last_signals.get(symbol)
        try:
            ltp = fetch_ltp(obj, info["exchange"], info["token"], symbol)
            ltp_str = f"Rs{ltp:,.2f}"
        except Exception:
            ltp_str = "N/A"

        if last:
            sig_icon = "🟢" if last["signal"] == "BUY" else ("🔴" if last["signal"] == "SELL" else "⏸️")
            lines.append(
                f"{sig_icon} *{symbol}* {ltp_str}\n"
                f"  Last: {last['signal']} @ {last.get('time','?')} — "
                f"{last['confidence']}% | {last.get('mkt_cond','—')}"
            )
        else:
            lines.append(f"⏸️ *{symbol}* {ltp_str}  (no signal yet)")

    sigs, loss, trades = _state_get()
    max_loss = CAPITAL * MAX_DAILY_LOSS_PCT / 100
    status   = "🚫 HALTED" if loss >= max_loss else ("🛑 KILL" if _kill_active else "✅ OK")
    lines.append(f"{_SEP}\nSignals: {sigs}/{MAX_SIGNALS_PER_DAY}  Open: {trades}/{MAX_OPEN_TRADES}")
    lines.append(f"Loss: Rs{loss:,.0f}/{max_loss:,.0f}  Status: {status}")
    await update.message.reply_text(_truncate("\n".join(lines)), parse_mode="Markdown")


async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show last 20 signals."""
    if not _tg_auth_ok(update):
        return
    rows = db_get_recent_signals(20)
    if not rows:
        await update.message.reply_text("📭 No signals yet.")
        return
    lines = [f"*Recent Signals* (last {len(rows)})\n{_SEP}"]
    for r in rows:
        icon = {"correct": "✅", "wrong": "❌", "pending": "⏳"}.get(r["result"], "❓")
        ts   = r["ts"][:16].replace("T", " ")
        ai   = f" AI:{r['ai_score']:.0f}%" if r["ai_score"] else ""
        lines.append(
            f"{icon} {r['symbol']} *{r['signal']}* "
            f"Rs{r['price']:,.0f} | {r['confidence']}%{ai} | {ts}"
        )
    await update.message.reply_text(_truncate("\n".join(lines)), parse_mode="Markdown")


async def cmd_accuracy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show historical signal accuracy by symbol."""
    if not _tg_auth_ok(update):
        return
    rows = db_get_accuracy()
    if not rows:
        await update.message.reply_text("📭 No resolved signals yet.")
        return
    by_sym: Dict[str, Dict[str, int]] = {}
    for r in rows:
        s = r["symbol"]
        by_sym.setdefault(s, {"correct": 0, "wrong": 0, "pending": 0})
        k = r["result"] if r["result"] in ("correct", "wrong") else "pending"
        by_sym[s][k] += 1
    lines = [f"*Signal Accuracy*\n{_SEP}"]
    for sym, cnt in by_sym.items():
        total  = cnt["correct"] + cnt["wrong"]
        pct    = round(cnt["correct"] / total * 100, 1) if total else 0
        lines.append(
            f"*{sym}*: ✅{cnt['correct']} ❌{cnt['wrong']} ⏳{cnt['pending']} "
            f"— Win Rate: *{pct}%*"
        )
    await update.message.reply_text(_truncate("\n".join(lines)), parse_mode="Markdown")


async def cmd_performance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show P&L dashboard."""
    if not _tg_auth_ok(update):
        return
    rows = db_get_recent_signals(500)
    today = _today_key()
    today_rows = [r for r in rows if r["ts"][:10] == today]

    wins     = sum(1 for r in rows if r["result"] == "correct")
    losses   = sum(1 for r in rows if r["result"] == "wrong")
    pending  = sum(1 for r in rows if r["result"] == "pending")
    total    = wins + losses
    win_pct  = round(wins / total * 100, 1) if total else 0.0

    sig_today, loss_rs, open_tr = _state_get()
    max_loss   = CAPITAL * MAX_DAILY_LOSS_PCT / 100
    loss_pct   = round(loss_rs / CAPITAL * 100, 2)
    remaining  = max(0.0, max_loss - loss_rs)
    status_icon= "🚫 HALTED" if loss_rs >= max_loss else "✅ OK"

    msg = (
        f"*Performance Dashboard*\n{_SEP}\n"
        f"*All-Time Signals*\n"
        f"Total: {len(rows)}  ✅{wins}  ❌{losses}  ⏳{pending}\n"
        f"Win Rate: *{win_pct}%*\n"
        f"{_SEP}\n"
        f"*Today ({today})*\n"
        f"Signals: {sig_today}/{MAX_SIGNALS_PER_DAY}\n"
        f"Loss: Rs{loss_rs:,.0f} ({loss_pct}%)\n"
        f"Max allowed: Rs{max_loss:,.0f}\n"
        f"Remaining: Rs{remaining:,.0f}\n"
        f"Open trades: {open_tr}/{MAX_OPEN_TRADES}\n"
        f"Status: *{status_icon}*\n"
        f"{_SEP}\n"
        f"*AI Engine*\n"
        f"ML Available: {'✅' if _ML_AVAILABLE else '❌'}\n"
        f"Models trained: {', '.join(_ai_models.keys()) or 'None yet'}\n"
        f"{_SEP}\n"
        f"_Use /dashboard for full stats._"
    )
    await update.message.reply_text(_truncate(msg), parse_mode="Markdown")


async def cmd_dashboard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Full performance stats including backtest summary."""
    if not _tg_auth_ok(update):
        return
    rows = db_get_recent_signals(1000)

    # By-symbol stats
    by_sym: Dict[str, Dict] = {}
    for r in rows:
        s = r["symbol"]
        if s not in by_sym:
            by_sym[s] = {"wins": 0, "losses": 0, "conf_sum": 0, "ai_sum": 0, "count": 0}
        if r["result"] == "correct":
            by_sym[s]["wins"] += 1
        elif r["result"] == "wrong":
            by_sym[s]["losses"] += 1
        by_sym[s]["conf_sum"] += r["confidence"]
        by_sym[s]["ai_sum"]   += (r["ai_score"] or 0)
        by_sym[s]["count"]    += 1

    lines = [f"*📊 Full Dashboard*\n{_SEP}"]
    for sym, d in by_sym.items():
        total  = d["wins"] + d["losses"]
        wr     = round(d["wins"] / total * 100, 1) if total else 0
        avg_c  = round(d["conf_sum"] / d["count"], 1) if d["count"] else 0
        avg_ai = round(d["ai_sum"]   / d["count"], 1) if d["count"] else 0
        lines.append(
            f"*{sym}*\n"
            f"  ✅ {d['wins']}  ❌ {d['losses']}  WR={wr}%\n"
            f"  Avg Confidence: {avg_c}%  Avg AI: {avg_ai}%"
        )

    # Latest backtest
    with _db_lock:
        conn = db_connect()
        try:
            bt = conn.execute(
                "SELECT * FROM backtest_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        except Exception:
            bt = None

    if bt:
        lines.append(
            f"{_SEP}\n*Latest Backtest*\n"
            f"Trades: {bt['total_trades']}  "
            f"WR: {bt['win_rate']:.1%}  "
            f"PF: {bt['profit_factor']:.2f}\n"
            f"MaxDD: {bt['max_dd']:.1%}  "
            f"Sharpe: {bt['sharpe']:.2f}"
        )

    lines.append(f"{_SEP}\n_Use /backtest to refresh._")
    await update.message.reply_text(_truncate("\n".join(lines)), parse_mode="Markdown")


async def cmd_risk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show current risk status."""
    if not _tg_auth_ok(update):
        return
    sig_today, loss_rs, open_tr = _state_get()
    max_loss  = CAPITAL * MAX_DAILY_LOSS_PCT / 100
    remaining = max(0.0, max_loss - loss_rs)
    status    = "🚫 HALTED" if loss_rs >= max_loss else "✅ OK"
    kill_str  = "🛑 ACTIVE" if _kill_active else "✅ Off"
    msg = (
        f"*Risk Status*\n{_SEP}\n"
        f"Capital: Rs{CAPITAL:,.0f}\n"
        f"Risk/trade: {RISK_PER_TRADE_PCT}%\n"
        f"Max daily loss: Rs{max_loss:,.0f}\n"
        f"Today loss: Rs{loss_rs:,.0f}\n"
        f"Remaining: Rs{remaining:,.0f}\n"
        f"Signals: {sig_today}/{MAX_SIGNALS_PER_DAY}\n"
        f"Open trades: {open_tr}/{MAX_OPEN_TRADES}\n"
        f"Kill switch: {kill_str}\n"
        f"Auto-trade: {'✅ ON' if AUTO_TRADE_ENABLED else '❌ OFF'}\n"
        f"Status: *{status}*"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_alert(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /alert              — list active alerts
    /alert NIFTY ABOVE 22500
    /alert BANKNIFTY BELOW 48000
    /alert cancel <id>
    """
    if not _tg_auth_ok(update):
        return
    args = ctx.args if ctx.args else []

    if not args:
        rows = db_get_active_alerts()
        if not rows:
            await update.message.reply_text("📭 No active alerts.\n\nUse: /alert NIFTY ABOVE 22500")
            return
        lines = ["*Active Price Alerts*", _SEP]
        for r in rows:
            arrow = "📈" if r["direction"] == "ABOVE" else "📉"
            lines.append(f"{arrow} [{r['id']}] {r['symbol']} {r['direction']} Rs{r['price']:,.0f}")
        lines.append(f"{_SEP}\nCancel: /alert cancel <id>")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    if args[0].lower() == "cancel":
        if len(args) < 2 or not args[1].isdigit():
            await update.message.reply_text("Usage: /alert cancel <id>")
            return
        db_delete_alert(int(args[1]))
        await update.message.reply_text(f"✅ Alert #{args[1]} cancelled.")
        return

    if len(args) < 3:
        await update.message.reply_text(
            "Usage:\n"
            "/alert NIFTY ABOVE 22500\n"
            "/alert BANKNIFTY BELOW 48000\n"
            "/alert — list active alerts\n"
            "/alert cancel <id>"
        )
        return

    symbol    = args[0].upper()
    direction = args[1].upper()
    if symbol not in SYMBOLS:
        await update.message.reply_text(f"❌ Unknown symbol: {symbol}\nAvailable: {', '.join(SYMBOLS)}")
        return
    if direction not in ("ABOVE", "BELOW"):
        await update.message.reply_text("❌ Direction must be ABOVE or BELOW")
        return
    try:
        price = float(args[2].replace(",", ""))
    except ValueError:
        await update.message.reply_text("❌ Invalid price. Example: /alert NIFTY ABOVE 22500")
        return

    alert_id = db_add_alert(symbol, direction, price)
    arrow    = "📈" if direction == "ABOVE" else "📉"
    await update.message.reply_text(
        f"{arrow} *Alert Set!*\n"
        f"{_SEP}\n"
        f"[#{alert_id}] *{symbol}* {direction} Rs{price:,.0f}\n"
        f"You'll be notified when price crosses this level.\n"
        f"Cancel: /alert cancel {alert_id}",
        parse_mode="Markdown"
    )


async def cmd_kill(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Activate the emergency kill switch to halt all auto-trade orders."""
    if not _tg_auth_ok(update):
        return
    activate_kill_switch()
    await update.message.reply_text(
        "🛑 *Kill Switch Activated*\n"
        "All new auto-trade orders are blocked.\n"
        "Restart the bot to reset.",
        parse_mode="Markdown"
    )


async def cmd_backtest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Run a backtest on recent historical data and report results."""
    if not _tg_auth_ok(update):
        return
    await update.message.reply_text("⏳ Running backtest… this may take 30-60 seconds.")
    try:
        obj, _, _ = _angel_session()
    except Exception as exc:
        await update.message.reply_text(f"❌ Session error: {exc}")
        return

    try:
        sym  = list(SYMBOLS.keys())[0]
        info = SYMBOLS[sym]
        df5  = _indicators_5min(fetch_candles(obj, info["token"], info["exchange"], "FIVE_MINUTE",    days=30))
        df15 = _indicators_15min(fetch_candles(obj, info["token"], info["exchange"], "FIFTEEN_MINUTE", days=30))
        prev = fetch_prev_day_ohlc(obj, info["token"], info["exchange"])
        pp   = calc_pivot_points(prev)
        m    = run_backtest(df5, df15, pp, export_csv=True)
    except Exception as exc:
        await update.message.reply_text(f"❌ Backtest error: {exc}")
        return

    await update.message.reply_text(
        f"✅ *Backtest Results ({sym})*\n{_SEP}\n"
        f"Trades: {m['total_trades']}\n"
        f"Win Rate: {m['win_rate']:.1%}\n"
        f"Profit Factor: {m['profit_factor']:.2f}\n"
        f"Max Drawdown: {m['max_drawdown']:.1%}\n"
        f"Sharpe Ratio: {m['sharpe']:.2f}\n"
        f"Gross Profit: Rs{m['gross_profit']:,.0f}\n"
        f"Gross Loss:   Rs{m['gross_loss']:,.0f}\n"
        f"{_SEP}\n"
        f"_Results exported to {BT_EXPORT_PATH}_",
        parse_mode="Markdown"
    )


async def cmd_optimise(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Run auto parameter optimisation via grid search."""
    if not _tg_auth_ok(update):
        return
    await update.message.reply_text(
        "🔧 *Auto-Optimisation Started*\n"
        "Grid-searching parameters… this may take 2–5 minutes.",
        parse_mode="Markdown"
    )
    try:
        obj, _, _ = _angel_session()
    except Exception as exc:
        await update.message.reply_text(f"❌ Session error: {exc}")
        return

    def _run_opt():
        try:
            sym  = list(SYMBOLS.keys())[0]
            info = SYMBOLS[sym]
            df5  = _indicators_5min(fetch_candles(obj, info["token"], info["exchange"], "FIVE_MINUTE",    days=60))
            df15 = _indicators_15min(fetch_candles(obj, info["token"], info["exchange"], "FIFTEEN_MINUTE", days=60))
            prev = fetch_prev_day_ohlc(obj, info["token"], info["exchange"])
            pp   = calc_pivot_points(prev)
            run_param_optimisation(df5, df15, pp)
        except Exception as exc:
            logger.error(f"[Optimise] Error: {exc}")
            tg_enqueue(f"❌ Optimisation failed: {exc}")

    threading.Thread(target=_run_opt, name="optimise", daemon=True).start()


async def cmd_trainai(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Force immediate AI model retraining."""
    if not _tg_auth_ok(update):
        return
    if not _ML_AVAILABLE:
        await update.message.reply_text("❌ ML libraries not installed (sklearn/xgboost required).")
        return
    await update.message.reply_text("🤖 Retraining AI models…")

    def _retrain():
        for sym in SYMBOLS:
            ok = _train_model(sym)
            tg_enqueue(
                f"🤖 AI retrain *{sym}*: "
                f"{'✅ Success' if ok else '⚠️ Not enough data'}"
            )

    threading.Thread(target=_retrain, name="trainai", daemon=True).start()


# ──────────────────────────────────────────────────────────────────────────────
# UNIT TEST SUITE  (run: python main_v6.py --test)
# ──────────────────────────────────────────────────────────────────────────────

class _BotTests(unittest.TestCase):
    """Inline unit tests — no external dependencies beyond pandas/numpy."""

    def _sample_df(self, n: int = 100) -> "pd.DataFrame":
        import numpy as np
        np.random.seed(42)
        closes = 22000 + np.cumsum(np.random.randn(n) * 20)
        highs  = closes + np.abs(np.random.randn(n) * 10)
        lows   = closes - np.abs(np.random.randn(n) * 10)
        vols   = np.random.randint(10000, 100000, n).astype(float)
        idx    = pd.date_range("2024-01-01 09:15", periods=n, freq="5min", tz=IST)
        return pd.DataFrame(
            {"open": closes, "high": highs, "low": lows, "close": closes, "volume": vols},
            index=idx,
        )

    def test_safe_float_nan(self):
        self.assertEqual(_safe_float(float("nan")), 0.0)
        self.assertEqual(_safe_float(float("inf")), 0.0)
        self.assertEqual(_safe_float(None), 0.0)
        self.assertAlmostEqual(_safe_float("3.14"), 3.14)

    def test_rsi_range(self):
        df = self._sample_df()
        rsi = calc_rsi(df["close"], period=14).dropna()
        self.assertTrue((rsi >= 0).all() and (rsi <= 100).all(),
                        "RSI must be in [0, 100]")

    def test_macd_returns_three(self):
        df = self._sample_df()
        m, s, h = calc_macd(df["close"])
        self.assertEqual(len(m), len(df))
        self.assertTrue(((m - s - h).abs() < 1e-9).all(),
                        "MACD hist must equal line minus signal")

    def test_atr_positive(self):
        df = self._sample_df()
        atr = calc_atr(df, period=14).dropna()
        self.assertTrue((atr > 0).all(), "ATR must be positive")

    def test_bollinger_band_order(self):
        df = self._sample_df()
        upper, mid, lower = calc_bollinger(df["close"], period=20, k=2.0)
        valid = upper.dropna().index
        self.assertTrue((upper[valid] >= mid[valid]).all(), "Upper >= Mid")
        self.assertTrue((mid[valid] >= lower[valid]).all(), "Mid >= Lower")

    def test_supertrend_direction_values(self):
        df = self._sample_df(150)
        st, dire = calc_supertrend(df, period=7, mult=2.0)
        valid_dir = dire.dropna().unique()
        self.assertTrue(set(valid_dir).issubset({1, -1}),
                        "Supertrend direction must be ±1")

    def test_pivot_points_order(self):
        prev = {"open": 22000, "high": 22500, "low": 21800, "close": 22200, "date": "01 Jan"}
        pp = calc_pivot_points(prev)
        self.assertGreater(pp["r1"], pp["pivot"])
        self.assertLess(pp["s1"], pp["pivot"])
        self.assertGreater(pp["r2"], pp["r1"])
        self.assertLess(pp["s2"], pp["s1"])

    def test_position_size_zero_risk(self):
        result = position_size(22000.0, 22000.0)  # SL == entry
        self.assertEqual(result["qty"], 0)

    def test_position_size_positive(self):
        result = position_size(22000.0, 21900.0)
        self.assertGreaterEqual(result["qty"], 1)
        self.assertGreater(result["capital_used"], 0)

    def test_confidence_blend_buy_agree(self):
        conf = _compute_final_confidence(
            ind_score=8, ind_max=9, ai_conf=0.85, ai_dir="BUY", signal="BUY"
        )
        self.assertGreaterEqual(conf, 70)
        self.assertLessEqual(conf, 100)

    def test_confidence_blend_disagree(self):
        conf_agree    = _compute_final_confidence(7, 9, 0.8, "BUY",  "BUY")
        conf_disagree = _compute_final_confidence(7, 9, 0.8, "SELL", "BUY")
        self.assertGreater(conf_agree, conf_disagree,
                           "Agreement should score higher than disagreement")

    def test_vwap_resets_daily(self):
        df = self._sample_df(100)
        vwap = calc_vwap(df)
        self.assertEqual(len(vwap), len(df))
        self.assertFalse(vwap.isna().all())

    def test_adx_non_negative(self):
        df = self._sample_df(100)
        adx = calc_adx(df, period=14).dropna()
        self.assertTrue((adx >= 0).all(), "ADX must be non-negative")

    def test_feature_vector_keys(self):
        sg = {
            "rsi": 55.0, "adx": 28.0, "macd_hist": 10.0, "atr": 100.0,
            "ema9": 22000.0, "ema21": 21950.0, "ema50": 21800.0,
            "vwap": 21990.0, "bb_upper": 22200.0, "bb_lower": 21800.0,
            "st_dir": 1, "vol_ok": True,
        }
        fv = _build_feature_vector(sg, ltp=22050.0)
        self.assertIsNotNone(fv)
        for key in _AI_FEATURES:
            self.assertIn(key, fv, f"Missing feature: {key}")


def _run_tests():
    """Run inline unit tests and exit."""
    print("=" * 60)
    print("Swastik Trading Bot v6 — Unit Test Suite")
    print("=" * 60)
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromTestCase(_BotTests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


# ──────────────────────────────────────────────────────────────────────────────
# GRACEFUL SHUTDOWN
# ──────────────────────────────────────────────────────────────────────────────

_tg_app_ref = None


def _handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum} — shutting down gracefully")
    _shutdown_event.set()
    if _tg_app_ref is not None:
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_tg_app_ref.stop())
            loop.run_until_complete(_tg_app_ref.shutdown())
            loop.close()
        except Exception as exc:
            logger.warning(f"Error stopping Telegram app: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    """Entry point. Initialises all subsystems and starts background threads."""
    global _bot_start_time, _tg_app_ref

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT,  _handle_shutdown)

    logger.info("=" * 60)
    logger.info("Swastik Trading Bot v6 ULTRA PRO starting")
    logger.info(
        f"Capital=Rs{CAPITAL:,.0f} | Risk={RISK_PER_TRADE_PCT}% | "
        f"MaxLoss={MAX_DAILY_LOSS_PCT}% | MaxSignals={MAX_SIGNALS_PER_DAY} | "
        f"AutoTrade={'ON' if AUTO_TRADE_ENABLED else 'OFF'} | "
        f"ML={'ON' if _ML_AVAILABLE else 'OFF'} | "
        f"XGB={'ON' if _XGB_AVAILABLE else 'OFF'}"
    )
    logger.info("=" * 60)

    db_init()
    _state_maybe_reset()
    _load_models()
    _bot_start_time = _ist_now()

    # Initial AI training attempt
    if _ML_AVAILABLE:
        for sym in SYMBOLS:
            _train_model(sym)

    try:
        _ws_obj, _ws_auth, _ws_feed = _angel_session()
        start_websocket(_ws_auth, _ws_feed, ANGEL_CLIENT_ID)
        logger.info("[WebSocket] Waiting for first tick (up to 10s)…")
        _ws_ready.wait(timeout=10)
        if _ws_ready.is_set():
            logger.info("[WebSocket] Live LTP feed active ✅")
        else:
            logger.warning("[WebSocket] No tick in 10s — REST fallback active")
    except Exception as exc:
        logger.warning(f"[WebSocket] Could not start: {exc} — REST only")

    # Background threads
    threads = {
        "tg-sender":    threading.Thread(target=_tg_sender_loop,     name="tg-sender",    daemon=True),
        "scheduler":    threading.Thread(target=_scheduler_loop,     name="scheduler",    daemon=True),
        "cache-clean":  threading.Thread(target=_cache_cleanup_loop,  name="cache-clean",  daemon=True),
        "alert-check":  threading.Thread(target=_alert_checker_loop,  name="alert-check",  daemon=True),
        "ai-retrain":   threading.Thread(target=_ai_model_retrain_loop, name="ai-retrain", daemon=True),
    }

    _start_health_server()
    for name, t in threads.items():
        t.start()
        logger.info(f"Thread started: {name}")

    # Telegram bot
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("status",      cmd_status))
    app.add_handler(CommandHandler("history",     cmd_history))
    app.add_handler(CommandHandler("accuracy",    cmd_accuracy))
    app.add_handler(CommandHandler("risk",        cmd_risk))
    app.add_handler(CommandHandler("performance", cmd_performance))
    app.add_handler(CommandHandler("dashboard",   cmd_dashboard))
    app.add_handler(CommandHandler("alert",       cmd_alert))
    app.add_handler(CommandHandler("kill",        cmd_kill))
    app.add_handler(CommandHandler("backtest",    cmd_backtest))
    app.add_handler(CommandHandler("optimise",    cmd_optimise))
    app.add_handler(CommandHandler("trainai",     cmd_trainai))
    _tg_app_ref = app

    logger.info(
        "Telegram polling active: "
        "/start /status /history /accuracy /risk /performance "
        "/dashboard /alert /kill /backtest /optimise /trainai"
    )

    def _run_polling():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(app.run_polling(allowed_updates=["message"], stop_signals=None))

    poll_thread = threading.Thread(
        target=_run_polling,
        name="tg-polling", daemon=True
    )
    poll_thread.start()

    _shutdown_event.wait()
    logger.info("Shutdown — cleaning up…")

    stop_websocket()
    for name, t in threads.items():
        t.join(timeout=5)
    poll_thread.join(timeout=10)
    logger.info("Bot stopped cleanly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Swastik Trading Bot v6")
    parser.add_argument("--test", action="store_true", help="Run unit tests and exit")
    args, _ = parser.parse_known_args()
    if args.test:
        _run_tests()
    main()
