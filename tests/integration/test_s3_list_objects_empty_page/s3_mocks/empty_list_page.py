"""A MinIO proxy that injects an empty `ListObjectsV2` page carrying a continuation token.

Amazon S3 may answer a listing request with no keys at all while still reporting
`IsTruncated=true` and a `NextContinuationToken`, because the scan can stop early inside a
partition. A client that treats "no keys" as "end of listing" then silently loses every object
that lives on a later page.

Once armed, the first listing of each distinct prefix is answered with such an empty page. The
real upstream answer is fetched at that moment and held back, then replayed when the client
returns with the token. Replaying instead of forwarding matters: the token is synthetic, and a
request rewritten to remove it would no longer match its AWS v4 signature.
"""

import http.client
import http.server
import socketserver
import sys
import threading
import urllib.parse

UPSTREAM_HOST = "minio1:9001"

# Marks a token as ours, so it is unambiguous when the client sends it back.
TOKEN_PREFIX = "injected-empty-page-"

state_lock = threading.Lock()
armed = False
injected_prefixes = set()
held_responses = {}
token_counter = 0


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


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def reply(self, code, body, content_type="text/plain"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def relay(self, r):
        self.send_response(r.status_code)
        for k, v in r.headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(r.content)

    def forward(self):
        content_length = self.headers.get("Content-Length")
        data = self.rfile.read(int(content_length)) if content_length else None
        return request(
            self.command,
            f"http://{UPSTREAM_HOST}{self.path}",
            headers=self.headers,
            data=data,
        )

    def empty_truncated_page(self, params, token):
        bucket = urllib.parse.urlparse(self.path).path.lstrip("/").split("/")[0]
        prefix = params.get("prefix", [""])[0]
        max_keys = params.get("max-keys", ["1000"])[0]
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Name>{bucket}</Name>"
            f"<Prefix>{prefix}</Prefix>"
            "<KeyCount>0</KeyCount>"
            f"<MaxKeys>{max_keys}</MaxKeys>"
            "<IsTruncated>true</IsTruncated>"
            f"<NextContinuationToken>{token}</NextContinuationToken>"
            "</ListBucketResult>"
        ).encode()

    def handle_list_request(self, params):
        global armed
        global token_counter

        token = params.get("continuation-token", [""])[0]

        if token.startswith(TOKEN_PREFIX):
            with state_lock:
                held = held_responses.pop(token, None)
            if held is not None:
                self.relay(held)
                return
            # Unknown token of ours: the client is following a page we no longer hold.
            self.reply(500, b"unknown injected continuation token")
            return

        prefix = params.get("prefix", [""])[0]
        with state_lock:
            inject = armed and prefix not in injected_prefixes
            if inject:
                injected_prefixes.add(prefix)

        if not inject:
            self.relay(self.forward())
            return

        upstream = self.forward()
        if upstream.status_code != 200:
            self.relay(upstream)
            return

        with state_lock:
            token_counter += 1
            token = f"{TOKEN_PREFIX}{token_counter}"
            held_responses[token] = upstream

        self.reply(200, self.empty_truncated_page(params, token), "application/xml")

    def do_GET(self):
        global armed
        global injected_prefixes
        global held_responses

        if self.path == "/":
            self.reply(200, b"OK")
            return

        if self.path.startswith("/arm") or self.path.startswith("/disarm"):
            with state_lock:
                armed = self.path.startswith("/arm")
                injected_prefixes = set()
                held_responses = {}
            self.reply(200, b"OK")
            return

        params = urllib.parse.parse_qs(
            urllib.parse.urlparse(self.path).query, keep_blank_values=True
        )
        if "list-type" in params:
            self.handle_list_request(params)
            return

        self.do_HEAD()

    def do_PUT(self):
        self.do_HEAD()

    def do_DELETE(self):
        self.do_HEAD()

    def do_POST(self):
        self.do_HEAD()

    def do_HEAD(self):
        self.relay(self.forward())


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle requests in a separate thread."""


httpd = ThreadedHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler)
httpd.serve_forever()
