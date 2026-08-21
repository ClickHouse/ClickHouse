import http.server
import sys

counter = 0
expected_path = "/test/test.txt"


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def process_head(self):
        global counter
        global expected_path

        current_auth = f"Bearer my-secret-token-{counter}"
        auth = self.headers.get("Authorization")

        if self.path.endswith("/ping"):
            self.send_response(200)
        elif not auth or auth != current_auth:
            self.send_response(403)
        elif self.path.endswith(expected_path):
            self.send_response(200)
        else:
            self.send_response(404)

        self.send_header("Content-Type", "text/plain")

        if self.path.endswith(expected_path):
            self.send_header("Content-Length", "2")

        self.end_headers()

    def do_HEAD(self):
        global counter
        self.process_head()
        counter += 1

    def do_GET(self):
        global counter
        global expected_path

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

        current_auth = f"Bearer my-secret-token-{counter}"
        auth = self.headers.get("Authorization")

        if not auth or auth != current_auth:
            self.wfile.write(b"Not authorized")
            return

        if not self.path.endswith(expected_path):
            self.wfile.write(b"Not found")
            return

        self.wfile.write(b"OK")
        counter += 1


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
