import http.server
import sys


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def get_response(self):
        if self.path == "/":
            return "OK", 200

        # Resource not found.
        return 404

    def check_request(self):
        content_type = self.headers.get("Content-Type")
        if content_type is None:
            return "No Content-Type", 400

        correct_content_type = self.headers.get("X-Test-Answer")
        if correct_content_type is None:
            return "No X-Test-Answer", 400

        if content_type != correct_content_type:
            return "Wrong Content-Type", 400

        return self.get_response()

    def do_POST(self):
        response, code = self.check_request()

        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", len(response.encode()))
        self.end_headers()
        self.wfile.write(response.encode())

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
