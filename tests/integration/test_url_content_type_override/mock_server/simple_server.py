import http.server
import sys


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def get_response(self):
        if self.path != "/":
            return "Wrong Path", 400

        content_type = self.headers.get("Content-Type")
        if content_type is None:
            return "No Content-Type", 400

        correct_content_type = self.headers.get("X-Test-Answer")
        if correct_content_type is None:
            return "No X-Test-Answer", 400

        if content_type != correct_content_type:
            return "Wrong Content-Type", 400

        return "OK", 200

    def do_POST(self):
        response, code = self.get_response()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", len(response.encode()))
        self.end_headers()
        return response, code


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
