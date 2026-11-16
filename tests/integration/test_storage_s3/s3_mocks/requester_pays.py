# Acts as a requester-pays bucket in front of MinIO: a request is forwarded (by a redirect) only if it
# carries a signed `x-amz-request-payer: requester` header, otherwise it is refused with 403.
# A forwarded request is first redirected through `/__redirected` of this mock, which records and checks it again;
# `GET /__requests` returns the recorded requests, `POST /__reset` clears them.
import http.server
import json
import re
import sys
import threading

UPSTREAM = "http://minio1:9001"
REDIRECTED = "/__redirected"

lock = threading.Lock()
recorded = []


class RequestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle_request(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)

        if self.command == "GET" and self.path == "/":
            self.reply(200, b"OK")
            return

        if self.path == "/__requests":
            with lock:
                body = json.dumps(recorded).encode()
            self.reply(200, body, "application/json")
            return

        if self.path == "/__reset":
            with lock:
                recorded.clear()
            self.reply(200, b"OK")
            return

        with lock:
            recorded.append({"path": self.path, "headers": self.headers.items()})

        signed = re.search(
            r"SignedHeaders=([^,]+)", self.headers.get("Authorization", "")
        )
        signed_headers = signed.group(1).split(";") if signed else []
        if (
            self.headers.get("x-amz-request-payer") != "requester"
            or "x-amz-request-payer" not in signed_headers
        ):
            body = (
                b""
                if self.command == "HEAD"
                else b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>'
            )
            self.reply(403, body, "application/xml")
            return

        if self.path.startswith(REDIRECTED + "/"):
            self.redirect(UPSTREAM + self.path[len(REDIRECTED) :])
        else:
            self.redirect(f"http://resolver:{sys.argv[1]}{REDIRECTED}{self.path}")

    def redirect(self, location):
        self.send_response(307)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

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
