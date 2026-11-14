import http.client
import http.server
import socketserver
import sys
import threading
import urllib.parse

# Forwards requests to minio and, like a requester-pays bucket, refuses every request without
# `x-amz-request-payer: requester`. Each forwarded or refused request is recorded for `GET /__requests`.
UPSTREAM_HOST = "minio1"
UPSTREAM_PORT = 9001

requests_lock = threading.Lock()
recorded_requests = []


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def reply(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_s3_request(self):
        content_length = self.headers.get("Content-Length")
        data = self.rfile.read(int(content_length)) if content_length else None

        query = urllib.parse.parse_qs(
            urllib.parse.urlparse(self.path).query, keep_blank_values=True
        )
        payer = self.headers.get("x-amz-request-payer")
        has_payer = payer == "requester"
        with requests_lock:
            recorded_requests.append(
                f"{self.command} {self.path} list={int('list-type' in query)} payer={payer if payer is not None else '-'}"
            )

        if not has_payer:
            body = (
                b""
                if self.command == "HEAD"
                else (
                    b'<?xml version="1.0" encoding="UTF-8"?>'
                    b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
                )
            )
            self.send_response(403)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        connection = http.client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT)
        connection.request(self.command, self.path, body=data, headers=self.headers)
        response = connection.getresponse()
        content = response.read()

        self.send_response(response.status)
        for name, value in response.headers.items():
            # The body is relayed de-chunked; a HEAD keeps the object size in `Content-Length`.
            if self.command != "HEAD" and name.lower() in (
                "transfer-encoding",
                "content-length",
            ):
                continue
            self.send_header(name, value)
        if self.command != "HEAD":
            self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self):
        if self.path == "/":
            self.reply(200, b"OK")
        elif self.path == "/__requests":
            with requests_lock:
                body = "".join(line + "\n" for line in recorded_requests)
            self.reply(200, body.encode())
        else:
            self.handle_s3_request()

    def do_POST(self):
        if self.path == "/__reset":
            with requests_lock:
                recorded_requests.clear()
            self.reply(200, b"OK")
        else:
            self.handle_s3_request()

    def do_HEAD(self):
        self.handle_s3_request()

    def do_PUT(self):
        self.handle_s3_request()

    def do_DELETE(self):
        self.handle_s3_request()


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle requests in a separate thread."""


httpd = ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
