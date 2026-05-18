import http.server
import sys


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def response_body(self):
        if self.path == "/":
            return b"OK"

        authorization = self.headers.get("Authorization", "")
        if "ADMIN_FAKE_KEY" in authorization:
            return b"1\n"
        if "ENV_FAKE_KEY" in authorization:
            return b"3\n"
        if "TRUSTED_FAKE_KEY" in authorization:
            return b"2\n"
        return b"0\n"

    def do_HEAD(self):
        body = self.response_body()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

    def do_GET(self):
        body = self.response_body()
        self.do_HEAD()
        if self.command != "HEAD":
            self.wfile.write(body)

    def log_message(self, format, *args):
        pass


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
