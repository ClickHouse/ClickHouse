import http.server
import json
import sys

counter = 0
expected_path = ""


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        global counter
        global expected_path

        if (
            self.path.endswith("/ping")
            or self.path.endswith("/counter")
            or self.path.endswith(expected_path)
        ):
            self.send_response(200)
        else:
            self.send_response(404)

        self.send_header("Content-Type", "text/json")
        self.end_headers()

    def do_GET(self):
        global counter
        global expected_path

        if self.path.endswith("/reset"):
            counter = 0
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        self.do_HEAD()
        if self.path.endswith("/ping"):
            self.wfile.write(b"OK")
            return

        if self.path.endswith("/counter"):
            self.wfile.write(str.encode(str(counter)))
            return

        if not self.path.endswith(expected_path):
            self.wfile.write(b"Not found")
            return

        token = {
            "access_token": f"my-secret-token-{counter}",
            "expires_in": 0,
            "token_type": "Bearer",
        }

        self.wfile.write(str.encode(json.dumps(token)))
        counter += 1


port = int(sys.argv[1])
token_path = sys.argv[2]
service_account = sys.argv[3]

expected_path = f"/{token_path}/{service_account}/token"
httpd = http.server.HTTPServer(("0.0.0.0", port), RequestHandler)
httpd.serve_forever()
