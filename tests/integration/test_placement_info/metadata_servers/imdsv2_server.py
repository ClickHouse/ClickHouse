import http.server
import sys
import uuid


# Mocks an IMDSv2-only EC2 instance metadata service:
#   * PUT /latest/api/token returns a fresh token when the
#     x-aws-ec2-metadata-token-ttl-seconds header is present;
#   * GET on metadata resources requires a matching x-aws-ec2-metadata-token
#     header and otherwise returns 401 (matching the AWS contract for
#     "IMDSv2 only (token required)" instances). Used to verify #81402.

ISSUED_TOKENS = set()


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_PUT(self):
        if self.path != "/latest/api/token":
            self.send_response(404)
            self.end_headers()
            return

        ttl = self.headers.get("x-aws-ec2-metadata-token-ttl-seconds")
        if ttl is None:
            self.send_response(400)
            self.end_headers()
            return

        # Drain any request body before responding.
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)

        token = uuid.uuid4().hex
        ISSUED_TOKENS.add(token)
        body = token.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        return self._serve_get_or_head(write_body=False)

    def do_GET(self):
        self._serve_get_or_head(write_body=True)

    def _serve_get_or_head(self, write_body):
        if self.path == "/":
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if write_body:
                self.wfile.write(body)
            return

        token = self.headers.get("x-aws-ec2-metadata-token")
        if not token or token not in ISSUED_TOKENS:
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if self.path == "/latest/meta-data/placement/availability-zone":
            body = b"ci-test-1a"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if write_body:
                self.wfile.write(body)
            return

        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()


httpd = http.server.HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
