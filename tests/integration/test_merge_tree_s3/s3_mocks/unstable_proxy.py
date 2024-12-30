import http.client
import http.server
import random
import socketserver
import sys
import urllib.parse

UPSTREAM_HOST = "minio1:9001"
random.seed("Unstable proxy/1.0")


def request(command, url, headers={}, data=None):
    """Mini-requests."""

    class Dummy:
        pass

    parts = urllib.parse.urlparse(url)
    c = http.client.HTTPConnection(parts.hostname, parts.port)
    c.request(
        command,
        urllib.parse.urlunparse(parts._replace(scheme="", netloc="")),
        headers=headers,
        body=data,
    )
    r = c.getresponse()
    result = Dummy()
    result.status_code = r.status
    result.headers = r.headers
    result.content = r.read()
    return result


class RequestHandler(http.server.BaseHTTPRequestHandler):
    # GetObject
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.do_HEAD()

    # PutObject
    def do_PUT(self):
        self.do_HEAD()

    # DeleteObjects (/root?delete)
    def do_POST(self):
        self.do_HEAD()

    # DeleteObject
    def do_DELETE(self):
        self.do_HEAD()

    def do_HEAD(self):
        content_length = self.headers.get("Content-Length")
        data = self.rfile.read(int(content_length)) if content_length else None
        r = request(
            self.command,
            f"http://{UPSTREAM_HOST}{self.path}",
            headers=self.headers,
            data=data,
        )
        self.send_response(r.status_code)
        for k, v in r.headers.items():
            self.send_header(k, v)
        self.end_headers()
        if random.random() < 0.25 and len(r.content) > 1024 * 1024:
            r.content = r.content[: len(r.content) // 2]
        self.wfile.write(r.content)


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle requests in a separate thread."""


httpd = ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
