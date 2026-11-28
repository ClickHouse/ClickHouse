import http.server
import sys
from urllib.parse import parse_qs, urlsplit

# Mimics the Blob service of Azure for one blob in a storage account with blob versioning enabled.
# The blob was deleted, so it has two previous versions and no current version: a request without
# `versionid` gets 404, as it does on Azure.
VERSIONS = {
    "2026-11-20T10:00:00.0000001Z": (b"1\n2\n3\n", '"0x8DA000000000001"'),
    "2026-11-20T10:00:01.0000002Z": (b"9\n", '"0x8DA000000000002"'),
}
LAST_MODIFIED = "Fri, 20 Nov 2026 10:00:00 GMT"


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def respond_text(self, status, body, with_body, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        if with_body:
            self.wfile.write(body)

    def respond_not_found(self, with_body):
        body = (
            b'<?xml version="1.0" encoding="utf-8"?>'
            b"<Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.</Message></Error>"
        )
        self.send_response(404)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("x-ms-error-code", "BlobNotFound")
        self.end_headers()
        if with_body:
            self.wfile.write(body)

    def respond(self, with_body):
        url = urlsplit(self.path)
        if url.path == "/":
            self.respond_text(200, b"OK", with_body)
            return

        query = parse_qs(url.query)
        if query.get("restype") == ["container"]:
            self.respond_text(
                200,
                b"",
                with_body,
                {
                    "ETag": '"0x8DA000000000000"',
                    "Last-Modified": LAST_MODIFIED,
                    "x-ms-lease-state": "available",
                    "x-ms-lease-status": "unlocked",
                    "x-ms-has-immutability-policy": "false",
                    "x-ms-has-legal-hold": "false",
                },
            )
            return

        version = query.get("versionid", [None])[0]
        if not url.path.endswith("/data.csv") or version not in VERSIONS:
            self.respond_not_found(with_body)
            return

        data, etag = VERSIONS[version]
        size = len(data)
        status = 200
        content_range = None
        range_header = self.headers.get("x-ms-range") or self.headers.get("Range")
        if range_header:
            begin, end = range_header.split("=")[1].split("-")
            begin = int(begin)
            end = min(int(end), size - 1) if end else size - 1
            content_range = f"bytes {begin}-{end}/{size}"
            data = data[begin : end + 1]
            status = 206

        self.send_response(status)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Last-Modified", LAST_MODIFIED)
        self.send_header("ETag", etag)
        self.send_header("x-ms-blob-type", "BlockBlob")
        self.send_header("x-ms-version-id", version)
        if content_range:
            self.send_header("Content-Range", content_range)
        self.end_headers()
        if with_body:
            self.wfile.write(data)

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

    def log_message(self, *args):
        pass


# Threaded: a pooled/idle client connection must not block the real request.
http.server.ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
