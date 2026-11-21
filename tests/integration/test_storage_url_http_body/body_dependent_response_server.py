import http.server
import sys

# A mock HTTP endpoint whose response depends on the request body. It is used to prove that the
# URL schema-inference cache and the URL row-count cache are partitioned by `body(...)`: the same
# URL must not serve a schema or a row count that was produced for a different body.
#
# The key is the exact request body string; the value is the response body returned for it.
RESPONSES = {
    # Different bodies -> different inferred schemas for the same URL.
    "schema_a": '{"col_a":1}\n',
    "schema_bc": '{"col_b":2,"col_c":3}\n',
    # Different bodies -> different row counts for the same URL (with a fixed schema).
    "rows_1": '{"v":7}\n',
    "rows_3": '{"v":7}\n{"v":7}\n{"v":7}\n',
}


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # for health-check in the docker startup script
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def do_POST(self):
        body = self.read_body().strip()
        data = RESPONSES.get(body, "").encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

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
