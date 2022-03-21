import http.server
import sys


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        if self.path.startswith("/get-my-path/"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()

        elif self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()

        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()


    def do_GET(self):
        self.do_HEAD()
        if self.path.startswith("/get-my-path/"):
            self.wfile.write(b'/' + self.path.split('/', maxsplit=2)[2].encode())

        elif self.path == "/":
            self.wfile.write(b"OK")


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
