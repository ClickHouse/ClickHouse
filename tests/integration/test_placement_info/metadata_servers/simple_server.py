import http.server
import sys


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def get_response(self):
        if self.path == "/":
            return "OK", 200

        if self.path == "/latest/meta-data/placement/availability-zone":
            return "ci-test-1a", 200

        # Resource not found.
        return 404

    def do_HEAD(self):
        response, code = self.get_response()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", len(response.encode()))
        self.end_headers()
        return response, code

    def do_GET(self):
        response, _ = self.do_HEAD()
        self.wfile.write(response.encode())


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
