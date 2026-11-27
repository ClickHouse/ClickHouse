import gzip
import http.server
import sys

# Simulates GCS decompressive transcoding: objects stored with Content-Encoding: gzip
# are served decompressed. HEAD omits Content-Length, GET returns plain text.
# Paths containing "encoded" simulate the non-transcoded case: the response keeps
# Content-Encoding: gzip and the payload stays compressed.
# See https://cloud.google.com/storage/docs/transcoding

DATA = b'{"id":1}\n{"id":2}\n{"id":3}\n'
GZIP_DATA = gzip.compress(DATA)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_object_headers(self):
        # No Content-Length in either case: the body is delimited by connection close
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("x-goog-stored-content-encoding", "gzip")
        if "encoded" in self.path:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("ETag", '"abc123"')
        self.send_header("Last-Modified", "Fri, 13 Mar 2026 10:54:50 GMT")
        self.send_header("Connection", "close")

    def do_HEAD(self):
        self.send_response(200)
        self.send_object_headers()
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
        self.send_object_headers()
        self.end_headers()
        self.wfile.write(GZIP_DATA if "encoded" in self.path else DATA)

    def log_message(self, *args):
        pass


# Threaded: a pooled/idle client connection must not block the real request.
http.server.ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
