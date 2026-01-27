import sys
from urllib.parse import parse_qs, urlparse
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
    "/data/order/a/part1.tsv": "10\n",
    "/data/order/b/part1.tsv": "20\n",
    "/data/headers/2025/part1.tsv": "7\n",
    "/data/headers/2025/part2.tsv": "8\n",
}


class RequestHandler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path in DATA_PARTS:
            data = DATA_PARTS[path].encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            return
        if path in (
            "/data/",
            "/data/2025/",
            "/data/empty/",
            "/data/query/",
            "/data/oversize/",
            "/data/glob/",
            "/data/order/",
            "/data/order/a/",
            "/data/order/b/",
            "/data/headers/",
            "/data/headers/2025/",
        ):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        if path == "/data/":
            body = "<a href=\"2025/\">2025/</a>\n"
            self._send_html(body)
            return

        if path == "/data/2025/":
            body = "<a href=\"part1.tsv\">part1.tsv</a>\n<a href=\"part2.tsv\">part2.tsv</a>\n"
            self._send_html(body)
            return
        if path == "/data/headers/":
            body = "<a href=\"2025/\">2025/</a>\n"
            self._send_html(body)
            return
        if path == "/data/headers/2025/":
            if self.headers.get("X-Test-Header") != "1":
                self.send_response(403)
                self.end_headers()
                return
            body = "<a href=\"part1.tsv\">part1.tsv</a>\n<a href=\"part2.tsv\">part2.tsv</a>\n"
            self._send_html(body)
            return
        if path == "/data/order/":
            body = "<a href=\"a/\">a/</a>\n<a href=\"b/\">b/</a>\n"
            self._send_html(body)
            return
        if path == "/data/order/a/":
            body = "<a href=\"part1.tsv\">part1.tsv</a>\n"
            self._send_html(body)
            return
        if path == "/data/order/b/":
            body = "<a href=\"part1.tsv\">part1.tsv</a>\n"
            self._send_html(body)
            return
        if path == "/data/glob/":
            body = (
                "<a href=\"parta.tsv\">parta.tsv</a>\n"
                "<a href=\"partb.tsv\">partb.tsv</a>\n"
                "<a href=\"partc.tsv\">partc.tsv</a>\n"
                "<a href=\"part1.tsv\">part1.tsv</a>\n"
                "<a href=\"part2.tsv\">part2.tsv</a>\n"
            )
            self._send_html(body)
            return
        if path == "/data/empty/":
            self._send_html("")
            return

        if path == "/data/query/":
            body = (
                "<a href=\"part3.tsv?download=1\">part3.tsv?download=1</a>\n"
                "<a href=\"part4.tsv#frag\">part4.tsv#frag</a>\n"
                "<a href=\"part5.tsv\">part5.tsv</a>\n"
            )
            self._send_html(body)
            return
        if path == "/data/oversize/":
            body = "a" * (10 * 1024 * 1024 + 1)
            self._send_html(body)
            return

        if path in DATA_PARTS:
            if path.startswith("/data/headers/") and self.headers.get("X-Test-Header") != "1":
                self.send_response(403)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(DATA_PARTS[path].encode("utf-8"))
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
