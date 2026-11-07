"""Proxy to the Lakekeeper REST catalog that shortens the reported expiry of vended S3 credentials."""

import json
import sys
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = "http://lakekeeper:8181"
EXPIRES_IN_MS = int(sys.argv[2])
SKIPPED_REQUEST_HEADERS = {"host", "accept-encoding", "connection", "content-length"}


def shorten_expiry(load_table_result):
    configs = [load_table_result.get("config") or {}]
    configs += [entry.get("config") or {} for entry in load_table_result.get("storage-credentials") or []]
    expires_at_ms = int(time.time() * 1000) + EXPIRES_IN_MS
    for config in configs:
        if "s3.access-key-id" in config:
            config["s3.session-token-expires-at-ms"] = str(expires_at_ms)


class RequestHandler(BaseHTTPRequestHandler):
    def proxy(self):
        if self.path == "/":
            self.respond(200, b"OK", "text/plain")
            return

        length = int(self.headers.get("Content-Length", 0))
        data = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIPPED_REQUEST_HEADERS}
        request = urllib.request.Request(UPSTREAM + self.path, data=data, headers=headers, method=self.command)
        try:
            with urllib.request.urlopen(request) as response:
                status, payload, content_type = response.status, response.read(), response.headers.get("Content-Type", "")
        except urllib.error.HTTPError as e:
            status, payload, content_type = e.code, e.read(), e.headers.get("Content-Type", "")

        if status == 200 and self.command == "GET" and "/tables/" in self.path:
            load_table_result = json.loads(payload)
            shorten_expiry(load_table_result)
            payload = json.dumps(load_table_result).encode()

        self.respond(status, payload, content_type)

    def respond(self, status, payload, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = do_POST = do_DELETE = proxy


ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler).serve_forever()
