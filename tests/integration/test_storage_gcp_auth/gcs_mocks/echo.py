"""Stand-in for the GCS XML (S3-interoperability) endpoint.

Usage: echo.py <port> [expected_token]

With `expected_token`, only that bearer token is accepted -- so a read that skipped an expected token exchange
fails instead of quietly succeeding.

Without it, the mock expects the rotating `my-secret-token-<n>` that `metadata.py` hands out for its rotating
service account, and counts requests, so a test can assert that the token was refreshed.

  GET /reset  -- zero the counter
"""

import http.server
import sys

EXPECTED_PATH = "/test/test.txt"

# None means the rotating mode, which is keyed on the request counter below.
STATIC_TOKEN = sys.argv[2] if len(sys.argv) > 2 else None

counter = 0


def expected_auth():
    if STATIC_TOKEN is not None:
        return f"Bearer {STATIC_TOKEN}"
    return f"Bearer my-secret-token-{counter}"


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def process_head(self):
        auth = self.headers.get("Authorization")

        if self.path.endswith("/ping"):
            self.send_response(200)
        elif auth != expected_auth():
            self.send_response(403)
        elif self.path.endswith(EXPECTED_PATH):
            self.send_response(200)
        else:
            self.send_response(404)

        self.send_header("Content-Type", "text/plain")

        if self.path.endswith(EXPECTED_PATH):
            self.send_header("Content-Length", "2")

        self.end_headers()

    def do_HEAD(self):
        global counter

        self.process_head()
        if STATIC_TOKEN is None:
            counter += 1

    def do_GET(self):
        global counter

        if self.path.endswith("/reset"):
            counter = 0
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        self.process_head()
        if self.path.endswith("/ping"):
            self.wfile.write(b"OK")
            return

        if self.headers.get("Authorization") != expected_auth():
            self.wfile.write(b"Not authorized")
            return

        if not self.path.endswith(EXPECTED_PATH):
            self.wfile.write(b"Not found")
            return

        self.wfile.write(b"OK")
        if STATIC_TOKEN is None:
            counter += 1

    def log_message(self, *args):
        pass


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
