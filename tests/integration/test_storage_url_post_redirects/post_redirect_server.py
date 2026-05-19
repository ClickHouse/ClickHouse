import http.server
import sys

REDIRECT_HOST = ""
REDIRECT_PORT = 0


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        # Health check used by the test harness.
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def _handle_write(self):
        # Consume the request body so the client sees the write as accepted
        # before we respond with the redirect.
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length:
            self.rfile.read(length)
        elif self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            while True:
                size_line = self.rfile.readline()
                if not size_line:
                    break
                size = int(size_line.strip(), 16)
                if size == 0:
                    # Trailer is empty; consume the final CRLF.
                    self.rfile.readline()
                    break
                self.rfile.read(size)
                self.rfile.readline()

        global REDIRECT_HOST, REDIRECT_PORT
        self.send_response(302)
        target = f"http://{REDIRECT_HOST}:{REDIRECT_PORT}{self.path}"
        self.send_header("Location", target)
        self.end_headers()

    def do_POST(self):
        self._handle_write()

    def do_PUT(self):
        self._handle_write()


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    REDIRECT_HOST = sys.argv[3]
    REDIRECT_PORT = int(sys.argv[4])
    httpd = http.server.ThreadingHTTPServer((host, port), RequestHandler)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
