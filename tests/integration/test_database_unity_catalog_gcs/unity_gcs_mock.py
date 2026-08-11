import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse
from xml.sax.saxutils import escape


STORAGE_PREFIX = "/storage/"
BUCKET = "bucket"
CATALOG_TOKEN = "Bearer catalog-token"
STORAGE_TOKEN = "Bearer gcp-token"


class Handler(BaseHTTPRequestHandler):
    storage_root = sys.argv[2]

    def log_message(self, format, *args):
        pass

    def send_body(self, status, body, content_type):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def send_json(self, value):
        self.send_body(200, json.dumps(value), "application/json")

    def check_authorization(self, expected):
        if self.headers.get("Authorization") == expected:
            return True
        self.send_body(403, "Forbidden", "text/plain")
        return False

    def handle_catalog_get(self, parsed):
        if not self.check_authorization(CATALOG_TOKEN):
            return

        if parsed.path.endswith("/schemas"):
            self.send_json({"schemas": [{"catalog_name": "warehouse", "full_name": "warehouse.namespace"}]})
            return

        if parsed.path.endswith("/tables"):
            self.send_json({"tables": [{"name": "table", "data_source_format": "DELTA"}]})
            return

        if parsed.path.endswith("/tables/warehouse.namespace.table"):
            self.send_json(
                {
                    "name": "table",
                    "table_id": "11111111-2222-3333-4444-555555555555",
                    "storage_location": "gs://bucket/table",
                    "data_source_format": "DELTA",
                    "columns": [{"name": "value", "nullable": False, "type_json": '"long"'}],
                }
            )
            return

        self.send_body(404, "Not found", "text/plain")

    def storage_path(self, key):
        normalized = os.path.normpath(unquote(key)).lstrip("/")
        path = os.path.join(self.storage_root, normalized)
        if os.path.commonpath([self.storage_root, path]) != self.storage_root:
            raise ValueError("Path escapes storage root")
        return path

    def list_objects(self, parsed):
        query = parse_qs(parsed.query)
        prefix = query.get("prefix", [""])[0]
        objects = []
        for root, _, files in os.walk(self.storage_root):
            for filename in files:
                path = os.path.join(root, filename)
                key = os.path.relpath(path, self.storage_root)
                if key.startswith(prefix):
                    objects.append((key, os.path.getsize(path)))

        contents = "".join(
            f"<Contents><Key>{escape(key)}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified>"
            f"<ETag>&quot;test&quot;</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
            for key, size in sorted(objects)
        )
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Name>{BUCKET}</Name><Prefix>{escape(prefix)}</Prefix><KeyCount>{len(objects)}</KeyCount>"
            f"<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
        )
        self.send_body(200, body, "application/xml")

    def get_object(self, key):
        try:
            with open(self.storage_path(key), "rb") as source:
                body = source.read()
        except (OSError, ValueError):
            self.send_body(404, "Not found", "text/plain")
            return

        range_header = self.headers.get("Range")
        if range_header and range_header.startswith("bytes="):
            start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
            start = int(start_text) if start_text else 0
            end = int(end_text) if end_text else len(body) - 1
            end = min(end, len(body) - 1)
            partial = body[start : end + 1]
            self.send_response(206)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(partial)))
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(partial)
            return

        self.send_body(200, body, "application/octet-stream")

    def handle_storage(self, parsed):
        if not self.check_authorization(STORAGE_TOKEN):
            return

        key = parsed.path.removeprefix(STORAGE_PREFIX)
        if key == BUCKET or key == BUCKET + "/":
            self.list_objects(parsed)
            return

        bucket_prefix = BUCKET + "/"
        if not key.startswith(bucket_prefix):
            self.send_body(404, "Not found", "text/plain")
            return
        self.get_object(key.removeprefix(bucket_prefix))

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_body(200, "OK", "text/plain")
        elif parsed.path.startswith("/api/2.1/unity-catalog/"):
            self.handle_catalog_get(parsed)
        elif parsed.path.startswith(STORAGE_PREFIX):
            self.handle_storage(parsed)
        else:
            self.send_body(404, "Not found", "text/plain")

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if not parsed.path.endswith("/temporary-table-credentials"):
            self.send_body(404, "Not found", "text/plain")
            return
        if not self.check_authorization(CATALOG_TOKEN):
            return
        self.send_json({"gcp_oauth_token": {"oauth_token": "gcp-token"}})


ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
