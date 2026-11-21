#!/usr/bin/env python3
"""A stand-in for an OPA server.

It answers decision queries from a rule loaded at startup and records every request body, so a test
can assert both the decision that was reached and the exact JSON that produced it. Using a stub
rather than a real OPA keeps these tests free of a Rego runtime; the policies themselves are
exercised by the separate cross-engine stack.

The decision rule is a Python expression evaluated with the request body bound to `input`. It is set
with `POST /rule` and defaults to allowing everything.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

state = {
    "rule": "True",
    "requests": [],
    "status": 200,
    "body_override": None,
    "row_filters": "{}",
    "column_masks": "{}",
    "batch": None,
    "batch_body": None,
}


class Handler(BaseHTTPRequestHandler):
    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length).decode() if length else ""

    def _respond(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _respond_raw(self, code, text):
        body = text.encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        body = self._read_body()

        # Control endpoints, used by the tests to steer the stub.
        if self.path == "/rule":
            state["rule"] = body or "True"
            state["requests"] = []
            state["status"] = 200
            state["body_override"] = None
            state["row_filters"] = "{}"
            state["column_masks"] = "{}"
            state["batch"] = None
            state["batch_body"] = None
            self._respond(200, {"ok": True})
            return

        if self.path == "/filters":
            # The row filter endpoint answers with a literal body, independently of the decision rule.
            state["row_filters"] = body or "{}"
            self._respond(200, {"ok": True})
            return

        if self.path == "/masks":
            state["column_masks"] = body or "{}"
            self._respond(200, {"ok": True})
            return

        if self.path == "/batch_body":
            state["batch_body"] = body
            self._respond(200, {"ok": True})
            return

        if self.path == "/batch":
            # A Python expression evaluated per resource, with `resource` bound. None means "answer the
            # batch from the decision rule instead".
            state["batch"] = body or None
            self._respond(200, {"ok": True})
            return

        if self.path == "/fail":
            # Makes every later decision query answer with a status the client must not read as allow.
            state["status"] = int(body or "500")
            self._respond(200, {"ok": True})
            return

        if self.path == "/body":
            # Returns a literal body, for asserting how a malformed response is handled.
            state["body_override"] = body
            self._respond(200, {"ok": True})
            return

        # Anything else is treated as a decision query.
        try:
            parsed = json.loads(body)
        except ValueError:
            self._respond(400, {"error": "not json"})
            return

        state["requests"].append(parsed)

        # The row filter endpoint is answered from its own state, so a filter response never has to
        # satisfy the decision endpoint's shape.
        if "rowFilters" in self.path:
            self._respond_raw(200, state["row_filters"])
            return

        if "columnMask" in self.path:
            self._respond_raw(200, state["column_masks"])
            return

        if "batch" in self.path:
            if state["batch_body"] is not None:
                self._respond_raw(200, state["batch_body"])
                return
            resources = parsed.get("input", {}).get("action", {}).get("filter_resources", [])
            rule = state["batch"] or state["rule"]
            allowed = []
            for index, resource in enumerate(resources):
                try:
                    keep = bool(eval(rule, {}, {"input": parsed.get("input", {}), "resource": resource}))
                except Exception as e:  # noqa: BLE001
                    self._respond(500, {"error": f"batch rule failed: {e!r}"})
                    return
                if keep:
                    allowed.append(index)
            self._respond(200, {"result": allowed})
            return

        if state["status"] != 200:
            self._respond(state["status"], {"error": "induced failure"})
            return

        if state["body_override"] is not None:
            self._respond_raw(200, state["body_override"])
            return

        try:
            decision = bool(eval(state["rule"], {}, {"input": parsed.get("input", {})}))
        except Exception as e:  # noqa: BLE001 - a broken rule must be reported, not crash the handler
            self._respond(500, {"error": f"rule failed: {e!r}"})
            return

        self._respond(200, {"result": decision})

    def do_GET(self):
        if self.path == "/requests":
            self._respond(200, state["requests"])
            return
        self._respond(404, {"error": "unknown"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8181
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
