import http.client
import http.server
import random
import socketserver
import sys
import urllib.parse

UPSTREAM_HOST = "minio1:9001"
random.seed("No list objects/1.0")


def request(command, url, headers={}, data=None):
    """Mini-requests."""

    class Dummy:
        pass

    parts = urllib.parse.urlparse(url)
    c = http.client.HTTPConnection(parts.hostname, parts.port)
    c.request(
        command,
        urllib.parse.urlunparse(parts._replace(scheme="", netloc="")),
        headers=headers,
        body=data,
    )
    r = c.getresponse()
    result = Dummy()
    result.status_code = r.status
    result.headers = r.headers
    result.content = r.read()
    return result


CUSTOM_AUTH_TOKEN_HEADER = "custom-auth-token"
CUSTOM_AUTH_TOKEN_VALID_VALUE = "ValidToken1234"


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return
        self.do_HEAD()

    def do_PUT(self):
        self.do_HEAD()

    def do_DELETE(self):
        self.do_HEAD()

    def do_POST(self):
        self.do_HEAD()

    def do_HEAD(self):

        custom_auth_token = self.headers.get(CUSTOM_AUTH_TOKEN_HEADER)
        if custom_auth_token and custom_auth_token != CUSTOM_AUTH_TOKEN_VALID_VALUE:
            self.send_response(403)
            self.send_header("Content-Type", "application/xml")
            self.end_headers()

            body = f"""<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>AccessDenied</Code>
    <Message>Access Denied. Custom token was {custom_auth_token}, the correct one: {CUSTOM_AUTH_TOKEN_VALID_VALUE}.</Message>
    <Resource>RESOURCE</Resource>
    <RequestId>REQUEST_ID</RequestId>
</Error>
"""
            self.wfile.write(body.encode())
            return

        content_length = self.headers.get("Content-Length")
        data = self.rfile.read(int(content_length)) if content_length else None
        r = request(
            self.command,
            f"http://{UPSTREAM_HOST}{self.path}",
            headers=self.headers,
            data=data,
        )
        self.send_response(r.status_code)
        for k, v in r.headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(r.content)
        self.wfile.close()


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle requests in a separate thread."""


httpd = ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
