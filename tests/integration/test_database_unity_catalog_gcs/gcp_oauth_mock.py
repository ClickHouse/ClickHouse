import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path == "/":
            body = b"OK"
            content_type = "text/plain"
        elif self.path == "/computeMetadata/v1/instance/service-accounts/default/token":
            body = json.dumps(
                {
                    "access_token": "gcp-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                }
            ).encode()
            content_type = "application/json"
        else:
            self.send_error(404)
            return

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


ThreadingHTTPServer(("0.0.0.0", 80), Handler).serve_forever()
