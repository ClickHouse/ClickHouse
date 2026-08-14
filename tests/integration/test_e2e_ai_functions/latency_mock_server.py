"""Threaded, delay-injecting, concurrency-instrumented OpenAI-compatible mock.

Separate from `test_ai_functions/mock_ai_server.py`, which is a single-threaded
`HTTPServer`: that one serializes every request and would report `max_in_flight = 1` no
matter what ClickHouse does. This one exists to measure the shape of
`T ~ rows x D` with `D` known and injected.

Endpoints:
  GET  /health                  - readiness probe, returns "OK"
  GET  /stats                   - counters since the last reset, control paths excluded
  POST /config                  - set behavior (JSON body), effective until /reset
  POST /reset                   - clear counters and restore defaults
  POST /v1/chat/completions     - chat completion, response shape as OpenAI
  POST /v1/embeddings           - one deterministic embedding per input

Config keys (all optional):
  delay_ms        int    fixed sleep before responding (default 0). `time.sleep` releases
                         the GIL, so threads scale for delay_ms > 0
  jitter_ms       int    uniform jitter added to the delay (default 0)
  max_concurrency int    simulated endpoint limit; 0 disables (default 0)
  over_limit      str    "429" to reject immediately, "queue" to wait for a slot
  reject_next_n   int    reject the next N data requests outright, independent of
                         concurrency. Needed because the chat path is serial, so a
                         concurrency limit alone may never be reached and a throttling
                         test would pass vacuously
  reject_status   int    status used by `reject_next_n` (default 429)
  output_tokens   int    words of filler content, so `max_tokens` behavior is testable
  embedding_dim   int    vector size when the request does not ask for `dimensions`
  echo_token      bool   echo a `TOK<n>` / `(ref NNNNNN)` marker found in the prompt, so
                         cross-talk between concurrent queries is detectable

Usage: python3 latency_mock_server.py [port]
"""

import http.server
import json
import random
import re
import socketserver
import sys
import threading
import time

DEFAULT_PORT = 18124
DEFAULT_EMBED_DIM = 8

CONTROL_PATHS = ("/health", "/stats", "/config", "/reset")

DEFAULT_CONFIG = {
    "delay_ms": 0,
    "jitter_ms": 0,
    "max_concurrency": 0,
    "over_limit": "429",
    "reject_next_n": 0,
    "reject_status": 429,
    "output_tokens": 8,
    "embedding_dim": DEFAULT_EMBED_DIM,
    "echo_token": False,
}

TOKEN_RE = re.compile(r"(TOK[A-Za-z0-9_]+|\(ref \d{6}\))")

LOCK = threading.Lock()
SLOT_FREED = threading.Condition(LOCK)

# Bumped by /reset. A keep-alive socket outlives a reset, so each connection records the
# epoch it was last counted in and is counted again after a reset if it sends more work.
RESET_EPOCH = 0

CONFIG = dict(DEFAULT_CONFIG)


def _new_stats():
    return {
        "requests": 0,
        "by_path": {},
        "in_flight": 0,
        "max_in_flight": 0,
        # Time-weighted area under the in-flight curve, for mean_in_flight.
        "in_flight_area_ns": 0,
        "last_change_ns": None,
        "connections": 0,
        "over_limit_rejections": 0,
        "first_request_ns": None,
        "last_response_ns": None,
    }


STATS = _new_stats()


def _accumulate_area_locked(now_ns):
    """Fold the interval since the last gauge change into the area. Caller holds LOCK."""
    if STATS["last_change_ns"] is not None:
        STATS["in_flight_area_ns"] += STATS["in_flight"] * (
            now_ns - STATS["last_change_ns"]
        )
    STATS["last_change_ns"] = now_ns


def _enter(path):
    """Account for an arriving request. Returns None when accepted, else a status code.

    A rejected request never enters the in-flight gauge, so `max_in_flight` can never
    exceed `max_concurrency`.
    """
    with LOCK:
        if CONFIG["reject_next_n"] > 0:
            CONFIG["reject_next_n"] -= 1
            STATS["over_limit_rejections"] += 1
            return CONFIG["reject_status"]

        limit = CONFIG["max_concurrency"]
        if limit:
            if CONFIG["over_limit"] == "queue":
                while STATS["in_flight"] >= limit:
                    SLOT_FREED.wait()
            elif STATS["in_flight"] >= limit:
                STATS["over_limit_rejections"] += 1
                return 429

        now_ns = time.monotonic_ns()
        _accumulate_area_locked(now_ns)
        STATS["requests"] += 1
        STATS["by_path"][path] = STATS["by_path"].get(path, 0) + 1
        STATS["in_flight"] += 1
        STATS["max_in_flight"] = max(STATS["max_in_flight"], STATS["in_flight"])
        if STATS["first_request_ns"] is None:
            STATS["first_request_ns"] = now_ns
        return None


def _leave():
    with LOCK:
        now_ns = time.monotonic_ns()
        _accumulate_area_locked(now_ns)
        STATS["in_flight"] -= 1
        STATS["last_response_ns"] = now_ns
        SLOT_FREED.notify()


def _snapshot():
    with LOCK:
        now_ns = time.monotonic_ns()
        area = STATS["in_flight_area_ns"]
        if STATS["last_change_ns"] is not None:
            area += STATS["in_flight"] * (now_ns - STATS["last_change_ns"])
        span = 0
        if STATS["first_request_ns"] is not None:
            end = STATS["last_response_ns"] or now_ns
            span = max(1, end - STATS["first_request_ns"])
        return {
            "requests": STATS["requests"],
            "by_path": dict(STATS["by_path"]),
            "in_flight": STATS["in_flight"],
            "max_in_flight": STATS["max_in_flight"],
            "mean_in_flight": round(area / span, 3) if span else 0.0,
            "connections": STATS["connections"],
            "over_limit_rejections": STATS["over_limit_rejections"],
            "first_request_ns": STATS["first_request_ns"],
            "last_response_ns": STATS["last_response_ns"],
            "elapsed_ms": round(span / 1e6, 3) if span else 0.0,
            "config": dict(CONFIG),
        }


def _sleep_configured():
    delay_ms = CONFIG["delay_ms"]
    jitter_ms = CONFIG["jitter_ms"]
    total = delay_ms + (random.uniform(0, jitter_ms) if jitter_ms else 0)
    if total > 0:
        time.sleep(total / 1000.0)


def extract_user_message(body):
    data = json.loads(body)
    for message in reversed(data.get("messages", [])):
        if message.get("role") == "user":
            return message.get("content", "")
    return ""


def extract_json_schema(body):
    response_format = json.loads(body).get("response_format")
    if not response_format or response_format.get("type") != "json_schema":
        return None
    return response_format.get("json_schema", {})


def build_structured_content(json_schema, user_message):
    """A JSON object satisfying the schema.

    Enum properties take their first value, which is what makes `aiClassify` output land
    inside its category list; other properties echo the prompt.
    """
    properties = json_schema.get("schema", {}).get("properties", {})
    result = {}
    for key, prop in properties.items():
        if "enum" in prop and prop["enum"]:
            result[key] = prop["enum"][0]
        else:
            result[key] = user_message
    return json.dumps(result)


def build_content(body):
    user_message = extract_user_message(body)
    json_schema = extract_json_schema(body)
    if json_schema:
        return build_structured_content(json_schema, user_message)
    if CONFIG["echo_token"]:
        found = TOKEN_RE.findall(user_message)
        if found:
            return " ".join(found)
        return user_message
    words = max(1, CONFIG["output_tokens"])
    return " ".join(f"w{i}" for i in range(words))


def make_chat_response(body):
    content = build_content(body)
    prompt_tokens = max(1, len(extract_user_message(body)) // 4)
    return {
        "choices": [{"message": {"content": content}, "finish_reason": "stop"}],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": max(1, len(content.split())),
        },
    }


def make_embedding_vector(text, dim):
    if not text:
        return [0.0] * dim
    return [
        round(((ord(text[i % len(text)]) * (i + 1)) % 1000) / 1000.0, 3)
        for i in range(dim)
    ]


def make_embeddings_response(body):
    data = json.loads(body)
    inputs = data.get("input", [])
    if isinstance(inputs, str):
        inputs = [inputs]
    dim = int(data.get("dimensions") or 0) or CONFIG["embedding_dim"]
    items = [
        {
            "object": "embedding",
            "index": index,
            "embedding": make_embedding_vector(text, dim),
        }
        for index, text in enumerate(inputs)
    ]
    tokens = sum(max(1, len(text) // 4) for text in inputs)
    return {
        "object": "list",
        "data": items,
        "model": data.get("model", "mock-model"),
        "usage": {"prompt_tokens": tokens, "total_tokens": tokens},
    }


class Handler(http.server.BaseHTTPRequestHandler):
    # Keep-alive matters: without it every request opens a socket and the
    # requests-per-connection metric would measure the mock, not ClickHouse.
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        # Counted on the first AI request rather than here: control calls (/config,
        # /reset, /stats) open sockets too, and counting those would corrupt the
        # requests-per-connection metric.
        self._counted_epoch = -1

    def do_GET(self):
        if self.path.startswith("/health"):
            self._send_raw(200, b"OK")
            return
        if self.path.startswith("/stats"):
            self._send_json(200, _snapshot())
            return
        self._send_json(404, {"error": {"message": f"unknown path {self.path}"}})

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        body = self._read_body()

        if path == "/config":
            self._apply_config(body)
            return
        if path == "/reset":
            self._reset()
            return

        self._count_connection()
        rejected_with = _enter(path)
        if rejected_with is not None:
            # 429 is what a real gateway sends; the url layer treats it as retriable.
            self._send_json(
                rejected_with,
                {"error": {"message": "rate limit exceeded", "type": "rate_limit"}},
                extra_headers={"Retry-After": "1"},
            )
            return
        try:
            _sleep_configured()
            if path == "/v1/chat/completions":
                self._send_json(200, make_chat_response(body))
            elif path == "/v1/embeddings":
                self._send_json(200, make_embeddings_response(body))
            else:
                self._send_json(
                    404, {"error": {"message": f"unknown path {path}"}}
                )
        finally:
            _leave()

    def _count_connection(self):
        global RESET_EPOCH
        with LOCK:
            if self._counted_epoch != RESET_EPOCH:
                self._counted_epoch = RESET_EPOCH
                STATS["connections"] += 1

    def _apply_config(self, body):
        try:
            requested = json.loads(body) if body else {}
        except ValueError as error:
            self._send_json(400, {"error": {"message": f"bad config: {error}"}})
            return
        unknown = sorted(set(requested) - set(DEFAULT_CONFIG))
        if unknown:
            self._send_json(400, {"error": {"message": f"unknown keys: {unknown}"}})
            return
        with LOCK:
            for key, value in requested.items():
                if isinstance(DEFAULT_CONFIG[key], bool):
                    CONFIG[key] = bool(value)
                elif isinstance(DEFAULT_CONFIG[key], int):
                    CONFIG[key] = int(value)
                else:
                    CONFIG[key] = str(value)
            # A narrower limit can leave waiters parked; wake them to re-check.
            SLOT_FREED.notify_all()
            snapshot = dict(CONFIG)
        self._send_json(200, {"config": snapshot})

    def _reset(self):
        global STATS, RESET_EPOCH
        with LOCK:
            in_flight = STATS["in_flight"]
            CONFIG.clear()
            CONFIG.update(DEFAULT_CONFIG)
            STATS = _new_stats()
            # In-flight requests still hold slots and will decrement on their way out.
            STATS["in_flight"] = in_flight
            RESET_EPOCH += 1
            SLOT_FREED.notify_all()
        self._send_json(200, {"reset": True})

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _send_json(self, status, payload, extra_headers=None):
        self._send_raw(
            status,
            json.dumps(payload).encode("utf-8"),
            content_type="application/json",
            extra_headers=extra_headers,
        )

    def _send_raw(self, status, body, content_type="text/plain", extra_headers=None):
        try:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            for name, value in (extra_headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Expected in the timeout cases: ClickHouse gives up on the socket while the
            # injected delay is still running, so the write lands on a closed connection.
            self.close_connection = True

    def log_message(self, fmt, *args):
        pass  # suppress per-request logging


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    # A queued request holds a worker thread, so the backlog has to be generous.
    request_queue_size = 128
    allow_reuse_address = True


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    server = ThreadedHTTPServer(("0.0.0.0", port), Handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
