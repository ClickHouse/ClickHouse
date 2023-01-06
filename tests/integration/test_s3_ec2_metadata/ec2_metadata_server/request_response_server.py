import http.server
import sys


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def get_response(self):
        if self.path == "/":
            return "OK"
        elif self.path == "/latest/meta-data/iam/security-credentials":
            return "myrole"
        elif self.path == "/latest/meta-data/iam/security-credentials/myrole":
            return '{ "Code" : "Success", "Type" : "AWS-HMAC", "AccessKeyId" : "minio", "SecretAccessKey" : "minio123" }'
        else:
            return None

    def do_HEAD(self):
        response = self.get_response()
        if response:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", len(response.encode()))
            self.end_headers()
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()

    def do_GET(self):
        self.do_HEAD()
        response = self.get_response()
        if response:
            self.wfile.write(response.encode())


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
