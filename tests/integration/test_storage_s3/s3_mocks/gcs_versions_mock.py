import http.server
import sys
from urllib.parse import parse_qs, urlsplit

# Mimics the XML API of GCS for one object in a bucket with object versioning enabled.
# The object was deleted, so it has two noncurrent versions (generations) and no live version:
# a request without `generation` gets 404, as it does on GCS.
# GCS names the version of an object `generation` and does not know the `versionId` of S3,
# so a request with `versionId` is rejected.
GENERATIONS = {
    "1700000000000001": (b"1\n2\n3\n", '"etag-1"'),
    "1700000000000002": (b"9\n", '"etag-2"'),
}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def respond_error(self, status, code, with_body):
        body = (
            f"<?xml version='1.0' encoding='UTF-8'?><Error><Code>{code}</Code><Message>{code}</Message></Error>"
        ).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if with_body:
            self.wfile.write(body)

    def respond(self, with_body):
        url = urlsplit(self.path)
        if url.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", "2")
            self.end_headers()
            if with_body:
                self.wfile.write(b"OK")
            return

        query = parse_qs(url.query)
        if "versionId" in query:
            self.respond_error(400, "InvalidArgument", with_body)
            return

        generation = query.get("generation", [None])[0]
        if generation is None:
            self.respond_error(404, "NoSuchKey", with_body)
            return
        if generation not in GENERATIONS:
            self.respond_error(404, "NoSuchVersion", with_body)
            return

        data, etag = GENERATIONS[generation]
        size = len(data)
        status = 200
        content_range = None
        range_header = self.headers.get("Range")
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
        self.send_header("Last-Modified", "Mon, 02 Nov 2026 00:00:00 GMT")
        self.send_header("ETag", etag)
        self.send_header("x-goog-generation", generation)
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
