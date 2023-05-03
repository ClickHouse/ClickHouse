import sys
import urllib.parse
import http.server
import socketserver


UPSTREAM_HOST = "minio1"
UPSTREAM_PORT = 9001


class ServerRuntime:
    def __init__(self):
        self.error_at_put_when_length_bigger = None

    def reset(self):
        self.error_at_put_when_length_bigger = None


runtime = ServerRuntime()


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def _ok(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def _ping(self):
        self._ok()

    def _redirect(self):
        content_length = int(self.headers.get("Content-Length", 0))
        to_read = content_length
        while to_read > 0:
            # read content in order to avoid error on client
            # Poco::Exception. Code: 1000, e.code() = 32, I/O error: Broken pipe
            # do it piece by piece in order to avoid big allocation
            size = min(to_read, 1024)
            str(self.rfile.read(size))
            to_read -= size

        self.send_response(307)
        url = f"http://{UPSTREAM_HOST}:{UPSTREAM_PORT}{self.path}"
        self.send_header("Location", url)
        self.end_headers()
        self.wfile.write(b"Redirected")

    def _error(self, data):
        self.send_response(500)
        self.send_header("Content-Type", "text/xml")
        self.end_headers()
        self.wfile.write(data)

    def _broken_settings(self):
        parts = urllib.parse.urlsplit(self.path)
        path = [x for x in parts.path.split("/") if x]
        assert path[0] == "mock_settings", path
        if path[1] == "error_at_put":
            params = urllib.parse.parse_qs(parts.query, keep_blank_values=False)
            runtime.error_at_put_when_length_bigger = int(
                params.get("when_length_bigger", [1024*1024])[0]
            )
            self._ok()
        elif path[1] == "reset":
            runtime.reset()
            self._ok()
        else:
            self._error("mock_settings, wrong command")

    def do_GET(self):
        if self.path == "/":
            self._ping()
        elif self.path.startswith("/mock_settings"):
            self._broken_settings()
        else:
            self._redirect()

    def do_PUT(self):
        if runtime.error_at_put_when_length_bigger is not None:
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length > runtime.error_at_put_when_length_bigger:
                self._error(
                    b'<?xml version="1.0" encoding="UTF-8"?>'
                    b"<Error>"
                    b"<Code>ExpectedError</Code>"
                    b"<Message>mock s3 injected error</Message>"
                    b"<RequestId>txfbd566d03042474888193-00608d7537</RequestId>"
                    b"</Error>"
                )
            else:
                self._redirect()
        else:
            self._redirect()

    def do_POST(self):
        self._redirect()

    def do_HEAD(self):
        self._redirect()

    def do_DELETE(self):
        self._redirect()


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle requests in a separate thread."""


httpd = ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
