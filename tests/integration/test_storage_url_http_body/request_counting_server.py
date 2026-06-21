import http.server
import sys
import threading

# A mock HTTP endpoint that counts how many POST requests it receives. It is used to document the
# request semantics of `body(...)`: when `structure` is omitted, the `url` table function sends one
# POST to infer the schema and one more to read the data (two POSTs); with an explicit `structure`,
# schema inference is skipped, so exactly one POST is sent.
#
# The current count is written to COUNT_PATH after every POST and can be reset via `GET /reset`.
COUNT_PATH = "/request_count.txt"

_lock = threading.Lock()
_count = 0

# A fixed response so the schema can be inferred for the no-structure case.
RESPONSE = b'{"v":1}\n'


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def _write_count(self):
        with open(COUNT_PATH, "w") as f:
            f.write(str(_count))

    def do_GET(self):
        global _count
        if self.path.startswith("/reset"):
            with _lock:
                _count = 0
                self._write_count()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def do_POST(self):
        global _count
        # Drain the request body so the connection stays healthy.
        self.read_body()
        with _lock:
            _count += 1
            self._write_count()
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Content-Length", str(len(RESPONSE)))
        self.end_headers()
        self.wfile.write(RESPONSE)

    def read_body(self):
        if self.headers.get("Transfer-Encoding") == "chunked":
            return self.read_chunked()
        content_length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(content_length).decode("utf-8", errors="replace")

    def read_chunked(self):
        chunks = []
        while True:
            chunk_size_line = self.rfile.readline().strip()
            if not chunk_size_line:
                break
            chunk_size = int(chunk_size_line, 16)
            if chunk_size == 0:
                self.rfile.readline()
                break
            chunks.append(self.rfile.read(chunk_size).decode("utf-8", errors="replace"))
            self.rfile.readline()
        return "".join(chunks)


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    httpd = http.server.ThreadingHTTPServer((host, port), RequestHandler)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
