import sys
import http.server

DATA = b'{"id":1}\n{"id":2}\n{"id":3}\n'


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_HEAD(self):
        self.send_response(200)
        # No Content-Length: simulates GCS decompressive transcoding
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("ETag", '"abc123"')
        self.send_header("Last-Modified", "Fri, 13 Mar 2026 10:54:50 GMT")
        self.send_header("Connection", "close")
        self.end_headers()

    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        # No Content-Length on GET either
        self.send_header("ETag", '"abc123"')
        self.send_header("Last-Modified", "Fri, 13 Mar 2026 10:54:50 GMT")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(DATA)

    def log_message(self, *args):
        pass


http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
