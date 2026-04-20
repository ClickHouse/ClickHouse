"""
Mock S3 server that introduces a configurable delay on PUT (write) requests
that target Iceberg metadata paths (URLs containing "/metadata/").  Data-file
PUTs are served immediately so that the data write succeeds while the metadata
write reliably times out, exercising the fix for
https://github.com/ClickHouse/ClickHouse/issues/98165.

Endpoints:
  GET /                              → "OK" (health/ping)
  GET /<bucket>?list-type=2&..      → empty ListObjectsV2 response
  HEAD /<bucket>/<key>              → 404 (object does not exist)
  POST /<bucket>/<key>?uploads      → initiates multipart upload, returns upload ID
  PUT /<bucket>/<key>               → delays WRITE_DELAY_SECONDS if path contains
                                       "/metadata/", otherwise responds immediately
  PUT /<bucket>/<key>?partNumber=N  → same delay rule as above
  POST /<bucket>/<key>?uploadId=X   → completes multipart upload
  DELETE /<bucket>/<key>?uploadId=X → aborts multipart upload
  PUT /mock_control/delay/<n>        → change the write delay to n seconds
  GET /mock_control/active_writes    → return number of in-flight write requests
"""

import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse

_delay_lock = threading.Lock()
_write_delay = 30.0

_active_writes_lock = threading.Lock()
_active_writes = 0


class Handler(BaseHTTPRequestHandler):
    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length > 0 else b""

    def _respond(self, code, body=b"", content_type="text/plain", extra_headers=None):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _delay_and_respond(self):
        """Consume the request body, sleep for the configured delay, then 200."""
        self._read_body()

        with _delay_lock:
            delay = _write_delay

        global _active_writes
        with _active_writes_lock:
            _active_writes += 1
        try:
            time.sleep(delay)
        finally:
            with _active_writes_lock:
                _active_writes -= 1

        self._respond(
            200,
            extra_headers={"ETag": '"d41d8cd98f00b204e9800998ecf8427e"'},
        )

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/":
            self._respond(200, b"OK")
            return

        if parsed.path == "/mock_control/active_writes":
            with _active_writes_lock:
                count = _active_writes
            self._respond(200, str(count).encode())
            return

        # S3 ListObjectsV2 → empty list (table does not exist yet)
        if "list-type" in params:
            body = (
                b'<?xml version="1.0" encoding="UTF-8"?>'
                b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b"<Name>bucket</Name><Prefix></Prefix>"
                b"<KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>"
                b"<IsTruncated>false</IsTruncated>"
                b"</ListBucketResult>"
            )
            self._respond(200, body, "application/xml")
            return

        # Any other GET (object read) → 404
        body = (
            b'<?xml version="1.0"?>'
            b"<Error><Code>NoSuchKey</Code>"
            b"<Message>The specified key does not exist.</Message></Error>"
        )
        self._respond(404, body, "application/xml")

    def do_HEAD(self):
        # All objects → 404 (no pre-existing data)
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_PUT(self):
        parsed = urlparse(self.path)
        parts = parsed.path.strip("/").split("/")

        # Control endpoint: PUT /mock_control/delay/<n>
        if (
            len(parts) >= 3
            and parts[0] == "mock_control"
            and parts[1] == "delay"
        ):
            try:
                new_delay = float(parts[2])
            except (ValueError, IndexError):
                self._read_body()
                self._respond(400, b"Bad Request")
                return
            self._read_body()
            global _write_delay
            with _delay_lock:
                _write_delay = new_delay
            self._respond(200, b"OK")
            return

        # Delay only Iceberg metadata writes ("/metadata/" in the path).
        # Data-file PUTs respond immediately so the data write succeeds and
        # the metadata write is the one that times out.
        if "/metadata/" in parsed.path:
            self._delay_and_respond()
        else:
            self._read_body()
            self._respond(
                200,
                extra_headers={"ETag": '"d41d8cd98f00b204e9800998ecf8427e"'},
            )

    def do_POST(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        self._read_body()

        # Initiate multipart upload: POST /<bucket>/<key>?uploads
        if "uploads" in params:
            path_parts = parsed.path.strip("/").split("/", 1)
            bucket = path_parts[0] if path_parts else "bucket"
            key = path_parts[1] if len(path_parts) > 1 else "key"
            upload_id = uuid.uuid4().hex
            resp = (
                f'<?xml version="1.0" encoding="UTF-8"?>'
                f'<InitiateMultipartUploadResult '
                f'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f"<Bucket>{bucket}</Bucket><Key>{key}</Key>"
                f"<UploadId>{upload_id}</UploadId>"
                f"</InitiateMultipartUploadResult>"
            ).encode()
            self._respond(200, resp, "application/xml")
            return

        # Complete multipart upload: POST /<bucket>/<key>?uploadId=X
        if "uploadId" in params:
            resp = (
                b'<?xml version="1.0" encoding="UTF-8"?>'
                b"<CompleteMultipartUploadResult "
                b'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b'<ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>'
                b"</CompleteMultipartUploadResult>"
            )
            self._respond(200, resp, "application/xml")
            return

        self._respond(404, b"Not Found")

    def do_DELETE(self):
        # Abort multipart upload or delete object → 204
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, fmt, *args):  # silence request logs
        pass


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each request in a separate thread."""

    daemon_threads = True


if __name__ == "__main__":
    port = int(sys.argv[1])
    server = ThreadedHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
