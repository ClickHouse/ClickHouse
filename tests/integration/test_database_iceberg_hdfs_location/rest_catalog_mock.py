"""Iceberg REST catalog that serves one table located on HDFS.

Only the endpoints `DataLakeCatalog` needs to resolve that table are implemented.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

NAMESPACE = "ns"
TABLE = "events"
LOCATION = f"hdfs://hdfs1:9000/warehouse/{NAMESPACE}/{TABLE}"

TABLE_METADATA = {
    "format-version": 2,
    "table-uuid": "0b8b3a3c-3c1e-4c2f-9d6a-1f4f3a2b5c6d",
    "location": LOCATION,
    "last-sequence-number": 0,
    "last-updated-ms": 1700000000000,
    "last-column-id": 2,
    "current-schema-id": 0,
    "schemas": [
        {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": True, "type": "long"},
                {"id": 2, "name": "name", "required": False, "type": "string"},
            ],
        }
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "last-partition-id": 999,
    "default-sort-order-id": 0,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {},
    "snapshots": [],
    "snapshot-log": [],
    "metadata-log": [],
}

ROUTES = {
    "/": "OK",
    "/v1/config": {"defaults": {}, "overrides": {}},
    f"/v1/namespaces/{NAMESPACE}/tables/{TABLE}": {
        "metadata-location": f"{LOCATION}/metadata/00000-0b8b3a3c.metadata.json",
        "metadata": TABLE_METADATA,
        # A real catalog returns no vended credentials for HDFS.
        "config": {},
    },
}


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = ROUTES.get(urlparse(self.path).path)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        if isinstance(body, str):
            payload = body.encode()
            content_type = "text/plain"
        else:
            payload = json.dumps(body).encode()
            content_type = "application/json"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


HTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler).serve_forever()
