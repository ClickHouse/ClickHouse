import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


DATA_PARTS = {
    "/data/2025/part1.tsv": "1\n2\n",
    "/data/2025/part2.tsv": "4\n5\n",
    "/data/query/part5.tsv": "3\n",
    "/data/glob/parta.tsv": "1\n",
    "/data/glob/partb.tsv": "2\n",
    "/data/glob/partc.tsv": "3\n",
    "/data/glob/part1.tsv": "4\n",
    "/data/glob/part2.tsv": "5\n",
}


class RequestHandler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        if self.path in DATA_PARTS:
            data = DATA_PARTS[self.path].encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            return
        if self.path in ("/data/", "/data/2025/", "/data/empty/", "/data/query/", "/data/oversize/", "/data/glob/"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

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
        if self.path == "/data/glob/":
            body = (
                "<a href=\"parta.tsv\">parta.tsv</a>\n"
                "<a href=\"partb.tsv\">partb.tsv</a>\n"
                "<a href=\"partc.tsv\">partc.tsv</a>\n"
                "<a href=\"part1.tsv\">part1.tsv</a>\n"
                "<a href=\"part2.tsv\">part2.tsv</a>\n"
            )
            self._send_html(body)
            return
        if self.path == "/data/empty/":
            self._send_html("")
            return

        if self.path == "/data/query/":
            body = (
                "<a href=\"part3.tsv?download=1\">part3.tsv?download=1</a>\n"
                "<a href=\"part4.tsv#frag\">part4.tsv#frag</a>\n"
                "<a href=\"part5.tsv\">part5.tsv</a>\n"
            )
            self._send_html(body)
            return

        if self.path == "/data/oversize/":
            body = "a" * (10 * 1024 * 1024 + 1)
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
