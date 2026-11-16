"""Metadata service with rotating and static token identities.

Usage: metadata.py <port> <token_path> <rotating_service_account> <static_service_account> <static_token>
ClickHouse uses port 80, so the two behaviors are selected by service account.
GET /counter and /reset expose the rotating token counter.
"""

import http.server
import json
import sys

counter = 0


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        if (
            self.path.endswith("/ping")
            or self.path.endswith("/counter")
            or self.path.endswith(rotating_path)
            or self.path.endswith(static_path)
        ):
            self.send_response(200)
        else:
            self.send_response(404)

        self.send_header("Content-Type", "text/json")
        self.end_headers()

    def do_GET(self):
        global counter

        if self.path.endswith("/reset"):
            counter = 0
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        self.do_HEAD()
        if self.path.endswith("/ping"):
            self.wfile.write(b"OK")
            return

        if self.path.endswith("/counter"):
            self.wfile.write(str.encode(str(counter)))
            return

        if self.path.endswith(static_path):
            token = {
                "access_token": static_token,
                "expires_in": 3600,
                "token_type": "Bearer",
            }
            self.wfile.write(str.encode(json.dumps(token)))
            return

        if not self.path.endswith(rotating_path):
            self.wfile.write(b"Not found")
            return

        token = {
            "access_token": f"my-secret-token-{counter}",
            "expires_in": 0,
            "token_type": "Bearer",
        }

        self.wfile.write(str.encode(json.dumps(token)))
        counter += 1

    def log_message(self, *args):
        pass


port = int(sys.argv[1])
token_path = sys.argv[2]
rotating_service_account = sys.argv[3]
static_service_account = sys.argv[4]
static_token = sys.argv[5]

rotating_path = f"/{token_path}/{rotating_service_account}/token"
static_path = f"/{token_path}/{static_service_account}/token"

httpd = http.server.HTTPServer(("0.0.0.0", port), RequestHandler)
httpd.serve_forever()
