import http.server
import sys


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def get_response(self):
        if self.path.startswith("/get-my-path/"):
            return b"/" + self.path.split("/", maxsplit=2)[2].encode()
        elif self.path == "/":
            return b"OK"

        return None

    def do_HEAD(self):
        if self.path.startswith("/get-my-path/") or self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", len(self.get_response()))
            self.end_headers()
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()

    def do_GET(self):
        self.do_HEAD()
        self.wfile.write(self.get_response())


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
