"""IAM Credentials mock: check the source token and target, then record the exchange.

Usage: iamcredentials.py <port> <expected_target_service_account>
GET /ping, /counter, /last_request and /reset expose readiness and request history.
"""

import http.server
import json
import sys

EXPECTED_SOURCE_AUTH = "Bearer source-token"

IMPERSONATED_TOKEN = "impersonated-token"

METHOD_SUFFIX = ":generateAccessToken"
RESOURCE_PREFIX = "/v1/projects/-/serviceAccounts/"

counter = 0
last_request = None


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def _respond(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _respond_text(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        global counter, last_request

        if self.path.endswith("/ping"):
            self._respond_text(200, b"OK")
        elif self.path.endswith("/counter"):
            self._respond_text(200, str(counter).encode())
        elif self.path.endswith("/last_request"):
            self._respond(200, last_request or {})
        elif self.path.endswith("/reset"):
            counter = 0
            last_request = None
            self._respond_text(200, b"OK")
        else:
            self._respond_text(404, b"Not found")

    def do_POST(self):
        global counter, last_request

        target_path = self.path

        if (
            not target_path.endswith(METHOD_SUFFIX)
            or RESOURCE_PREFIX not in target_path
        ):
            self._respond(404, {"error": {"message": f"Not found: {target_path}"}})
            return

        if self.headers.get("Authorization") != EXPECTED_SOURCE_AUTH:
            self._respond(
                401, {"error": {"message": "Missing or unexpected source credentials"}}
            )
            return

        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")

        resource = target_path[
            target_path.index(RESOURCE_PREFIX) + len(RESOURCE_PREFIX) :
        ]
        target = resource[: -len(METHOD_SUFFIX)]

        last_request = {
            "target": target,
            "scope": body.get("scope"),
            "lifetime": body.get("lifetime"),
            "delegates": body.get("delegates"),
            "content_type": self.headers.get("Content-Type"),
        }

        if target != expected_target:
            self._respond(
                403, {"error": {"message": f"Not permitted to impersonate {target}"}}
            )
            return

        counter += 1
        self._respond(
            200, {"accessToken": IMPERSONATED_TOKEN, "expireTime": expire_time()}
        )

    def log_message(self, *args):
        pass


def expire_time():
    from datetime import datetime, timedelta, timezone

    expires_at = datetime.now(timezone.utc) + timedelta(seconds=3600)
    return expires_at.strftime("%Y-%m-%dT%H:%M:%SZ")


port = int(sys.argv[1])
expected_target = sys.argv[2]

httpd = http.server.HTTPServer(("0.0.0.0", port), RequestHandler)
httpd.serve_forever()
