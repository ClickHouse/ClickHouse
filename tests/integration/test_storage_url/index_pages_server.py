import sys
import json
import re
from urllib.parse import urlparse
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
    "/data/mixed_headers/part1.tsv": "1\n",
    "/data/mixed_headers/part2.tsv": "2\n",
}


class RequestHandler(BaseHTTPRequestHandler):
    request_counts = {}

    def log_message(self, format, *args):
        pass

    @classmethod
    def _record_request(cls, method, path):
        key = f"{method} {path}"
        cls.request_counts[key] = cls.request_counts.get(key, 0) + 1

    def _handle_control(self):
        if self.path == "/__stats__":
            body = json.dumps(self.request_counts, sort_keys=True)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))
            return True

        if self.path == "/__reset__":
            self.request_counts.clear()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"OK")
            return True

        return False

    @staticmethod
    def _deep_directory_response(path):
        if path == "/data/deep/":
            return "<a href=\"0/\">0/</a>\n"

        if not re.fullmatch(r"/data/deep/(?:\d+/)+", path):
            return None

        levels = [level for level in path.removeprefix("/data/deep/").split("/") if level]
        if len(levels) >= 10:
            return ""
        next_level = len(levels)
        return f"<a href=\"{next_level}/\">{next_level}/</a>\n"

    def do_HEAD(self):
        if self._handle_control():
            return

        parsed = urlparse(self.path)
        path = parsed.path
        self._record_request("HEAD", path)
        if path in DATA_PARTS:
            data = DATA_PARTS[path].encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(data)))
            if path.startswith("/data/mixed_headers/"):
                self.send_header("X-Source-File", path.rsplit("/", 1)[-1])
            self.end_headers()
            return
        if path in (
            "/data/",
            "/data/2025/",
            "/data/deep/",
            "/data/empty/",
            "/data/query/",
            "/data/oversize/",
            "/data/glob/",
            "/data/order/",
            "/data/order/a/",
            "/data/order/b/",
            "/data/headers/",
            "/data/headers/2025/",
            "/data/mixed_headers/",
        ):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            return
        if re.fullmatch(r"/data/deep/(?:\d+/)+", path):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_GET(self):
        if self._handle_control():
            return

        parsed = urlparse(self.path)
        path = parsed.path
        self._record_request("GET", path)
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", "2")
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
        deep_body = self._deep_directory_response(path)
        if deep_body is not None:
            self._send_html(deep_body)
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
        if path == "/data/mixed_headers/":
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
            data = DATA_PARTS[path].encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(data)))
            if path.startswith("/data/mixed_headers/"):
                self.send_header("X-Source-File", path.rsplit("/", 1)[-1])
            self.end_headers()
            self.wfile.write(data)
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
