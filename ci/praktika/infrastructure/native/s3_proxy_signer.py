#!/usr/bin/env python3
"""Minimal SigV4 signing proxy for the Praktika S3 report proxy.

Caddy fronts this process for TLS + Tailscale; this process signs every
request with the EC2 instance role (resolved from IMDS by the standard AWS
credential chain) and streams the object back from S3. The proxied buckets
stay fully private — no anonymous access, no static credentials on disk.

Listens on 127.0.0.1:<port> and only serves GET/HEAD for objects under the
buckets named in PRAKTIKA_S3_PROXY_BUCKETS (space-separated allowlist).
"""
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
from botocore.exceptions import ClientError

_ALLOWED = set(
    b for b in os.environ.get("PRAKTIKA_S3_PROXY_BUCKETS", "").split() if b
)
_REGION = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or None
_S3 = boto3.client("s3", region_name=_REGION)

# S3 response attribute -> HTTP header. Only response metadata that a browser
# or curl needs to render/resume a download is forwarded.
_PASS_THROUGH = [
    ("ContentType", "Content-Type"),
    ("ContentLength", "Content-Length"),
    ("ETag", "ETag"),
    ("ContentRange", "Content-Range"),
    ("AcceptRanges", "Accept-Ranges"),
    ("CacheControl", "Cache-Control"),
    ("ContentEncoding", "Content-Encoding"),
    ("ContentDisposition", "Content-Disposition"),
]


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _target(self):
        path = self.path.split("?", 1)[0].lstrip("/")
        bucket, _, key = path.partition("/")
        return bucket, key

    def _s3_kwargs(self, bucket, key):
        kwargs = {"Bucket": bucket, "Key": key}
        rng = self.headers.get("Range")
        if rng:
            kwargs["Range"] = rng
        return kwargs

    def _empty(self, code):
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_metadata(self, resp, status):
        self.send_response(status)
        for attr, header in _PASS_THROUGH:
            value = resp.get(attr)
            if value is not None:
                self.send_header(header, str(value))
        self.end_headers()

    def _guard(self, bucket, key):
        if not bucket or not key or bucket not in _ALLOWED:
            self._empty(403)
            return False
        return True

    def do_HEAD(self):
        bucket, key = self._target()
        if not self._guard(bucket, key):
            return
        try:
            resp = _S3.head_object(**self._s3_kwargs(bucket, key))
        except ClientError as e:
            return self._empty(_status_of(e))
        self._send_metadata(resp, 200)

    def do_GET(self):
        bucket, key = self._target()
        if not self._guard(bucket, key):
            return
        try:
            resp = _S3.get_object(**self._s3_kwargs(bucket, key))
        except ClientError as e:
            return self._empty(_status_of(e))
        status = 206 if resp.get("ContentRange") else 200
        self._send_metadata(resp, status)
        body = resp["Body"]
        try:
            for chunk in body.iter_chunks(256 * 1024):
                self.wfile.write(chunk)
        finally:
            body.close()

    def log_message(self, *args):
        # Silence per-request logging; systemd/journald captures start/stop.
        pass


def _status_of(error):
    meta = getattr(error, "response", {}).get("ResponseMetadata", {})
    return int(meta.get("HTTPStatusCode") or 502)


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8081
    ThreadingHTTPServer((host, port), _Handler).serve_forever()


if __name__ == "__main__":
    main()
