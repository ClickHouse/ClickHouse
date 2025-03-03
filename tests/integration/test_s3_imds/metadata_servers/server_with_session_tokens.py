import http.server
import sys
import uuid

# Session tokens for IMDS sessions.
tokens = set()


def new_token():
    token = str(uuid.uuid4())
    global tokens
    tokens.add(token)
    return token


def token_exists(token):
    global tokens
    return token in tokens


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def get_response(self):
        if self.path == "/":
            return "OK", 200

        if self.path == "/latest/api/token":
            return new_token(), 200

        if self.path == "/latest/meta-data/iam/security-credentials":
            if token_exists(self.headers.get("x-aws-ec2-metadata-token")):
                return "myrole", 200

        if self.path == "/latest/meta-data/iam/security-credentials/myrole":
            if token_exists(self.headers.get("x-aws-ec2-metadata-token")):
                return (
                    '{ "Code" : "Success", "Type" : "AWS-HMAC", "AccessKeyId" : "minio", "SecretAccessKey" : "minio123" }',
                    200,
                )

        if self.path.startswith("/latest/meta-data/iam/security-credentials"):
            return "", 401  # Unknown token or not specified.

        # Resource not found.
        return "", 404

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

    def do_PUT(self):
        self.do_GET()


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
