#!/usr/bin/env python3
"""S3 fault-injection / list-anomaly proxy for the CA scenario suite (S22, S27).

Sits between ClickHouse and RustFS: ClickHouse's `ca` disk endpoint points at this proxy, which
forwards every request verbatim to the real RustFS upstream. SigV4 signs the `host` header, so the
proxy MUST preserve the client's Host header on the forwarded request; RustFS validates the signature
against the received Host, so forwarding it unchanged keeps auth valid while the TCP connection goes
to the upstream container.

Two fault families, both DISARMED by default (rate 0) so cluster bring-up + the CA capability probe
are never disturbed. The scenario ARMS faults for the workload window via the control port, then
disarms before the checkpoint.

- S22 (fault injection): with probability `rate`, a matched request gets a bounded transient fault:
  `503 SlowDown` / `429 SlowDown` (S3-style retryable body), artificial latency (`slow`), or a
  mid-response connection close (`reset`). Applied to GET/PUT/HEAD/POST/LIST per `methods`.
- S27 (list anomaly): for `LIST` (GET with `list-type=2`) whose `prefix` matches `list_prefix`,
  rewrite the returned XML to inject a duplicate key, or drop the continuation token — so the CA GC
  discovery/token-diff path must treat the page as ambiguous and re-read (never skip a fold).

Focused tests can additionally restrict S22 faults to a `path_substring` and an atomic
`remaining_faults` budget. The `drop_after_forward` mode records the upstream result and request
body digest, then closes the downstream connection so retry recovery can prove response-loss cases.

Control plane (separate port): POST /config {json}, GET /stats, GET /healthz. Deterministic: fault
decisions are driven by a seeded PRNG keyed per-request-index, so a given (seed, rate) is reproducible.
"""

import hashlib
import http.client
import http.server
import json
import os
import random
import socket
import socketserver
import sys
import threading
import time

UPSTREAM = os.environ.get("RUSTFS_UPSTREAM", "rustfs1:11121")  # host:port of the real store
S3_PORT = int(os.environ.get("S3_PROXY_PORT", "11121"))
CTL_PORT = int(os.environ.get("S3_PROXY_CTL_PORT", "8474"))

# Runtime-mutable fault config (guarded by _cfg_lock). rate=0 => pure pass-through.
_cfg_lock = threading.Lock()
_DEFAULT_CFG = {
    "rate": 0.0,                       # fraction of matched requests that get a fault
    "modes": ["503"],                  # subset of {503,429,slow,reset,drop_after_forward}
    "methods": ["GET", "PUT", "HEAD", "POST"],  # HTTP methods eligible for S22 faults
    "slow_ms": 1500,                   # latency for the "slow" mode
    "seed": 1,
    "path_substring": None,            # optional request-path scope
    "remaining_faults": None,          # optional exact finite fault budget; None = legacy unlimited
    # S27 list-anomaly config (independent of the S22 fault rate):
    "list_anomaly": None,              # None | "duplicate" | "drop_token"
    "list_prefix": "roots/",           # only LIST calls whose prefix contains this are perturbed
}
_DEFAULT_STATS = {
    "forwarded": 0,
    "faults": 0,
    "list_perturbed": 0,
    "by_mode": {},
    "drop_after_forward": [],
}
_cfg = dict(_DEFAULT_CFG)
_stats = dict(_DEFAULT_STATS)
_stats["by_mode"] = {}
_stats["drop_after_forward"] = []
_req_index = [0]
_idx_lock = threading.Lock()


def _get_cfg():
    with _cfg_lock:
        return dict(_cfg)


def _next_index():
    with _idx_lock:
        _req_index[0] += 1
        return _req_index[0]


def _bump(stat, key=None):
    with _cfg_lock:
        if key is None:
            _stats[stat] = _stats.get(stat, 0) + 1
        else:
            _stats[stat][key] = _stats[stat].get(key, 0) + 1


def _record_drop_after_forward(method, path, body, upstream_status, upstream_etag):
    record = {
        "method": method,
        "path": path,
        "request_body_sha256": hashlib.sha256(body).hexdigest(),
        "upstream_status": upstream_status,
        "upstream_etag": upstream_etag,
    }
    with _cfg_lock:
        records = _stats["drop_after_forward"]
        records.append(record)
        del records[:-64]


def _reset():
    with _cfg_lock:
        _cfg.clear()
        _cfg.update(_DEFAULT_CFG)
        _stats.clear()
        _stats.update(_DEFAULT_STATS)
        _stats["by_mode"] = {}
        _stats["drop_after_forward"] = []
        with _idx_lock:
            _req_index[0] = 0


_SLOWDOWN_BODY = (b'<?xml version="1.0" encoding="UTF-8"?>'
                  b'<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message>'
                  b'<Resource>/</Resource><RequestId>fault-proxy</RequestId></Error>')


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass  # quiet

    # --- fault decision -----------------------------------------------------
    def _should_fault(self, cfg):
        # Keep the original unscoped decision path byte-for-byte: existing scenario configs omit
        # both new fields and therefore retain their request-index/seed behavior.
        if cfg.get("path_substring") is None and cfg.get("remaining_faults") is None:
            if cfg["rate"] <= 0 or self.command not in cfg["methods"]:
                return None
            idx = _next_index()
            rng = random.Random(f"{cfg['seed']}:{idx}")
            if rng.random() < cfg["rate"]:
                return rng.choice(cfg["modes"]) if cfg["modes"] else None
            return None

        # A scoped finite rule is one atomic decision. In particular, concurrent matching requests
        # cannot all observe the same positive remaining count and over-consume the configured budget.
        with _cfg_lock:
            current = _cfg
            if current["rate"] <= 0 or self.command not in current["methods"]:
                return None
            path_substring = current.get("path_substring")
            if path_substring is not None and path_substring not in (self.path or ""):
                return None
            remaining = current.get("remaining_faults")
            if remaining is not None and remaining <= 0:
                return None
            idx = _next_index()
            rng = random.Random(f"{current['seed']}:{idx}")
            if rng.random() >= current["rate"]:
                return None
            mode = rng.choice(current["modes"]) if current["modes"] else None
            if mode is not None and remaining is not None:
                current["remaining_faults"] = remaining - 1
            return mode

    def _emit_fault(self, mode):
        _bump("faults")
        _bump("by_mode", mode)
        if mode in ("503", "429"):
            code = 503 if mode == "503" else 429
            self.send_response(code)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(_SLOWDOWN_BODY)))
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            self.wfile.write(_SLOWDOWN_BODY)
        elif mode == "slow":
            time.sleep(_get_cfg()["slow_ms"] / 1000.0)
            self._forward()  # after the delay, serve the real response
        elif mode == "reset":
            # Abruptly close the connection with no valid response -> client sees a transport error.
            try:
                self.close_connection = True
                self.connection.close()
            except Exception:
                pass
        elif mode == "drop_after_forward":
            self._forward(drop_after_forward=True)

    # --- request body -------------------------------------------------------
    def _read_body(self):
        length = self.headers.get("Content-Length")
        if length is not None:
            return self.rfile.read(int(length))
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            # De-chunk into a flat body (dev-scale payloads are small).
            data = bytearray()
            while True:
                line = self.rfile.readline().strip()
                if not line:
                    continue
                size = int(line.split(b";")[0], 16)
                if size == 0:
                    self.rfile.readline()  # trailing CRLF
                    break
                data += self.rfile.read(size)
                self.rfile.readline()
            return bytes(data)
        return b""

    # --- forward to upstream ------------------------------------------------
    def _forward(self, drop_after_forward=False):
        body = getattr(self, "_cached_body", None)
        if body is None:
            body = self._read_body()
        cfg = _get_cfg()
        conn = http.client.HTTPConnection(UPSTREAM, timeout=60)
        # Preserve headers verbatim (incl. Host, so SigV4 stays valid); strip hop-by-hop + Expect.
        # Expect: 100-continue MUST be dropped: http.client sends the body immediately (no 100 wait),
        # so relaying Expect makes the upstream reply with an interim 100 that getresponse() would
        # misread — corrupting the upload (observed: size-0 blobs on >=64 KiB PUTs). We already
        # buffered the full body and send it directly, so no 100-continue negotiation is needed.
        _HOP = {"transfer-encoding", "expect", "connection", "keep-alive", "proxy-connection",
                "te", "trailer", "upgrade"}
        fwd_headers = {}
        for k, v in self.headers.items():
            if k.lower() in _HOP:
                continue
            fwd_headers[k] = v
        fwd_headers["Content-Length"] = str(len(body))
        try:
            conn.request(self.command, self.path, body=body, headers=fwd_headers)
            resp = conn.getresponse()
            data = resp.read()
        except Exception as e:
            self.send_response(502)
            msg = f"proxy upstream error: {e}".encode()
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            conn.close()
            return
        # S27: perturb LIST XML if configured and this is a matching list call.
        if (cfg.get("list_anomaly") and self.command == "GET"
                and "list-type=2" in (self.path or "") and cfg["list_prefix"] in _decode_prefix(self.path)):
            perturbed = _perturb_list_xml(data, cfg["list_anomaly"])
            if perturbed is not None:
                data = perturbed
                _bump("list_perturbed")
        _bump("forwarded")
        if os.environ.get("S3_PROXY_DEBUG") and self.command in ("HEAD", "POST"):
            print(f"[dbg-resp] {self.command} {self.path[:50]} -> {resp.status} "
                  f"upstreamCL={resp.getheader('Content-Length')} bodylen={len(data)}", flush=True)
        if drop_after_forward:
            _record_drop_after_forward(
                self.command,
                self.path,
                body,
                resp.status,
                resp.getheader("ETag") or "",
            )
            conn.close()
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
            return
        self.send_response(resp.status)
        is_head = (self.command == "HEAD")
        for k, v in resp.getheaders():
            # For HEAD, rustfs returns the object's real Content-Length with an EMPTY body — preserve
            # it verbatim (the CA dedup probe reads it). For methods with a body, we resend the actual
            # byte count below. Always drop hop-by-hop framing headers.
            if k.lower() in ("transfer-encoding", "connection"):
                continue
            if k.lower() == "content-length" and not is_head:
                continue
            self.send_header(k, v)
        if not is_head:
            self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if not is_head:
            self.wfile.write(data)
        conn.close()

    def _handle(self):
        cfg = _get_cfg()
        # Cache body once (fault paths + forward both may need it).
        try:
            self._cached_body = self._read_body()
        except Exception:
            self._cached_body = b""
        if os.environ.get("S3_PROXY_DEBUG"):
            print(f"[dbg] {self.command} {self.path[:60]} CL={self.headers.get('Content-Length')} "
                  f"TE={self.headers.get('Transfer-Encoding')} CE={self.headers.get('Content-Encoding')} "
                  f"Expect={self.headers.get('Expect')} read={len(self._cached_body)}", flush=True)
        mode = self._should_fault(cfg)
        if mode is None:
            self._forward()
        else:
            self._emit_fault(mode)

    do_GET = _handle
    do_PUT = _handle
    do_POST = _handle
    do_HEAD = _handle
    do_DELETE = _handle


def _decode_prefix(path):
    # extract the `prefix=` query value (URL-encoded); good enough to match "roots/"
    import urllib.parse
    q = urllib.parse.urlparse(path).query
    params = urllib.parse.parse_qs(q)
    return urllib.parse.unquote(params.get("prefix", [""])[0])


def _perturb_list_xml(xml_bytes, anomaly):
    """Inject a LIST-page anomaly. 'duplicate' repeats the first <Contents> key; 'drop_token' removes
    the continuation token so the client cannot prove it saw the whole listing. Returns perturbed
    bytes, or None if there was nothing to perturb (caller keeps the original)."""
    try:
        s = xml_bytes.decode("utf-8", "replace")
    except Exception:
        return None
    if anomaly == "duplicate":
        i = s.find("<Contents>")
        j = s.find("</Contents>")
        if i == -1 or j == -1:
            return None
        block = s[i:j + len("</Contents>")]
        return (s[:j + len("</Contents>")] + block + s[j + len("</Contents>"):]).encode()
    if anomaly == "drop_token":
        import re
        out = re.sub(r"<NextContinuationToken>.*?</NextContinuationToken>", "", s)
        out = re.sub(r"<IsTruncated>true</IsTruncated>", "<IsTruncated>false</IsTruncated>", out)
        return out.encode()
    return None


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class CtlHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/healthz"):
            return self._json(200, {"ok": True, "upstream": UPSTREAM})
        if self.path.startswith("/stats"):
            with _cfg_lock:
                snapshot = dict(_stats)
                snapshot["by_mode"] = dict(_stats["by_mode"])
                snapshot["drop_after_forward"] = list(_stats["drop_after_forward"])
            return self._json(200, snapshot)
        self._json(404, {"error": "not found"})

    def do_POST(self):
        if not self.path.startswith("/config"):
            return self._json(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length", "0"))
        try:
            patch = json.loads(self.rfile.read(length) or b"{}")
        except Exception as e:
            return self._json(400, {"error": f"bad json: {e}"})
        reset = bool(patch.pop("reset", False))
        if reset:
            _reset()
        with _cfg_lock:
            _cfg.update(patch)
            snap = dict(_cfg)
        self._json(200, {"ok": True, "config": snap})


def main():
    s3 = ThreadingHTTPServer(("0.0.0.0", S3_PORT), Handler)
    ctl = ThreadingHTTPServer(("0.0.0.0", CTL_PORT), CtlHandler)
    print(f"[s3_fault_proxy] S3 :{S3_PORT} -> {UPSTREAM}; control :{CTL_PORT}", flush=True)
    threading.Thread(target=ctl.serve_forever, daemon=True).start()
    s3.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
