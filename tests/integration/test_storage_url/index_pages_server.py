import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


DATA_PARTS = {
    "/data/2025/part1.tsv": "1\n2\n",
    "/data/2025/part2.tsv": "4\n5\n",
}


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        if self.path == "/data/":
            body = "<a href=\"2025/\">2025/</a>\n"
            self._send_html(body)
            return

        if self.path == "/data/2025/":
            body = "<a href=\"part1.tsv\">part1.tsv</a>\n<a href=\"part2.tsv\">part2.tsv</a>\n"
            self._send_html(body)
            return

        if self.path in DATA_PARTS:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(DATA_PARTS[self.path].encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()

    def _send_html(self, body):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))


if __name__ == "__main__":
    port = int(sys.argv[1])
    httpd = HTTPServer(("0.0.0.0", port), RequestHandler)
    httpd.serve_forever()
