# Acts as a requester-pays bucket in front of MinIO: a request is forwarded (by a redirect) only if it
# carries a signed `x-amz-request-payer: requester` header, otherwise it is refused with 403.
import http.server
import re
import sys

UPSTREAM = "http://minio1:9001"


class RequestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle_request(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)

        if self.command == "GET" and self.path == "/":
            self.reply(200, b"OK")
            return

        signed = re.search(
            r"SignedHeaders=([^,]+)", self.headers.get("Authorization", "")
        )
        signed_headers = signed.group(1).split(";") if signed else []
        if (
            self.headers.get("x-amz-request-payer") == "requester"
            and "x-amz-request-payer" in signed_headers
        ):
            self.send_response(307)
            self.send_header("Location", UPSTREAM + self.path)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        body = (
            b""
            if self.command == "HEAD"
            else b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>'
        )
        self.reply(403, body, "application/xml")

    def reply(self, code, body, content_type="text/plain"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request


http.server.ThreadingHTTPServer(
    ("0.0.0.0", int(sys.argv[1])), RequestHandler
).serve_forever()
