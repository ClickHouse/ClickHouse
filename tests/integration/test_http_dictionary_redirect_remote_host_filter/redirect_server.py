import http.server
import os
import sys

# Marker file written when (and only when) the redirect target `/data` is actually served.
# The test reads it back via `/followed` to detect whether ClickHouse followed the redirect.
FOLLOWED_MARKER = "/redirect_followed.txt"


class RequestHandler(http.server.BaseHTTPRequestHandler):
    # Port the redirect target should point at; set from argv before the server starts.
    redirect_port = None

    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path == "/":
            self._respond(200, b'{"status":"ok"}', "text/plain")
        elif self.path == "/redirect":
            # 302 to the SAME server but via a different host string (`127.0.0.2`, another
            # loopback address), which is intentionally NOT listed in
            # <remote_url_allow_hosts>. A correctly-behaving server must reject this target
            # instead of following it.
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.2:{self.redirect_port}/data")
            self.end_headers()
        elif self.path == "/data":
            # Record that the redirect was actually followed (the bypass / SSRF happened).
            with open(FOLLOWED_MARKER, "w") as f:
                f.write("yes")
            self._respond(200, b"1\tfirst\n", "text/tab-separated-values")
        elif self.path == "/followed":
            followed = os.path.exists(FOLLOWED_MARKER)
            self._respond(200, b"YES" if followed else b"NO", "text/plain")
        else:
            self.send_response(404)
            self.end_headers()

    def _respond(self, code, body, content_type):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    RequestHandler.redirect_port = port
    httpd = http.server.ThreadingHTTPServer((host, port), RequestHandler)

    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
