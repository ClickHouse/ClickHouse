"""A stub Google OAuth 2.0 token endpoint for the native GCS refresh-token tests.

`POST /token` answers a `refresh_token` grant with a fresh bearer token and counts the exchange, so a
test can tell whether the transport minted one token for good or renewed it. `GET /count` reports the
number of exchanges so far together with the last request body, which is how a test checks that the
credentials the SQL definition supplied are the ones actually presented to the token endpoint.

The reported lifetime comes from the command line. Anything at or below
`GoogleOAuthAccessTokenExpirationSlack()` (4 minutes) makes google-cloud-cpp's caching decorator treat
the token as expiring soon and refresh it on every use; a long lifetime exercises the cache instead.
"""

import http.server
import json
import sys


class TokenHandler(http.server.BaseHTTPRequestHandler):
    exchanges = 0
    last_body = ""
    expires_in = 30

    def _respond(self, payload, content_type="application/json"):
        body = payload.encode()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/":
            self._respond("OK", "text/plain")
        elif self.path == "/count":
            self._respond(
                json.dumps(
                    {
                        "exchanges": TokenHandler.exchanges,
                        "last_body": TokenHandler.last_body,
                    }
                )
            )
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path != "/token":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        TokenHandler.last_body = self.rfile.read(length).decode()
        TokenHandler.exchanges += 1
        self._respond(
            json.dumps(
                {
                    "access_token": f"stub-access-token-{TokenHandler.exchanges}",
                    "token_type": "Bearer",
                    "expires_in": TokenHandler.expires_in,
                }
            )
        )

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))


if __name__ == "__main__":
    port = int(sys.argv[1])
    if len(sys.argv) > 2:
        TokenHandler.expires_in = int(sys.argv[2])
    http.server.ThreadingHTTPServer(("0.0.0.0", port), TokenHandler).serve_forever()
