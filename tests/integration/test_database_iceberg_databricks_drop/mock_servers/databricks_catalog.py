#!/usr/bin/env python3
"""A Databricks workspace as seen by a `DataLakeCatalog` database with `catalog_type = 'rest'`:

- the Iceberg REST catalog under `/api/2.1/unity-catalog/iceberg-rest`, with prefix `catalogs/unity`;
- the Unity tables API `GET /api/2.1/unity-catalog/tables/unity.<schema>.<table>`, which reports the
  `table_type` of every table.

Every route except `/` and `/control/*` requires `Authorization: Bearer <TOKEN>`.
`POST /control/reset` takes `{table_name: table_type}` for schema `SCHEMA` (an empty type is not reported)
and clears the request log,
`GET /control/requests` returns the log as a list of `"<METHOD> <path>?<query>"`.
"""
import json
import re
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

# Must match the constants in test.py.
TOKEN = "dapi-test-token"
CATALOG = "unity"
SCHEMA = "sch#\u00e9"

UNITY_TABLE = re.compile(r"^/api/2\.1/unity-catalog/tables/(?P<full_name>[^/]+)$")
REST_CONFIG = re.compile(r"^.*/v1/config$")
REST_NAMESPACES = re.compile(rf"^.*/v1/catalogs/{CATALOG}/namespaces$")
REST_TABLES = re.compile(rf"^.*/v1/catalogs/{CATALOG}/namespaces/(?P<namespace>[^/]+)/tables$")
REST_TABLE = re.compile(rf"^.*/v1/catalogs/{CATALOG}/namespaces/(?P<namespace>[^/]+)/tables/(?P<table>[^/]+)$")

LOCK = threading.Lock()
TABLES = {}
REQUESTS = []


def table_metadata(table_name):
    location = f"file:///var/lib/clickhouse/user_files/databricks_mock/{table_name}"
    return {
        "metadata-location": f"{location}/metadata/v1.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": str(uuid.uuid5(uuid.NAMESPACE_DNS, table_name)),
            "location": location,
            "last-sequence-number": 0,
            "last-updated-ms": 1700000000000,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [{"id": 1, "name": "id", "required": False, "type": "long"}],
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
        },
        "config": {},
    }


class RequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, status, body=None):
        payload = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        if body is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def not_found(self, error_type, message):
        self.reply(404, {"error": {"message": message, "type": error_type, "code": 404}})

    def read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def handle_control(self, method, path):
        if method == "POST" and path == "/control/reset":
            tables = json.loads(self.read_body() or b"{}")
            with LOCK:
                TABLES.clear()
                TABLES.update(tables)
                REQUESTS.clear()
            self.reply(200, {})
        elif method == "GET" and path == "/control/requests":
            with LOCK:
                self.reply(200, list(REQUESTS))
        else:
            self.reply(404, {})

    def handle_request(self, method):
        url = urlsplit(self.path)
        path = unquote(url.path)

        if method == "GET" and path == "/":
            payload = b"OK"
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if path.startswith("/control/"):
            self.handle_control(method, path)
            return

        self.read_body()
        with LOCK:
            REQUESTS.append(f"{method} {path}?{url.query}" if url.query else f"{method} {path}")

        if self.headers.get("Authorization", "").strip() != f"Bearer {TOKEN}":
            self.reply(401, {"error_code": "UNAUTHENTICATED", "message": "Invalid access token"})
            return

        if method == "GET" and (match := UNITY_TABLE.match(path)):
            parts = match.group("full_name").split(".")
            with LOCK:
                table_type = TABLES.get(parts[2]) if len(parts) == 3 and parts[:2] == [CATALOG, SCHEMA] else None
            if table_type is None:
                self.reply(404, {"error_code": "TABLE_DOES_NOT_EXIST", "message": f"Table '{match.group('full_name')}' does not exist."})
                return
            table = {
                "name": parts[2],
                "catalog_name": CATALOG,
                "schema_name": SCHEMA,
                "full_name": match.group("full_name"),
                "data_source_format": "ICEBERG",
            }
            # An empty type stands for a table the Unity API reports without `table_type`.
            if table_type:
                table["table_type"] = table_type
            self.reply(200, table)
        elif method == "GET" and REST_CONFIG.match(path):
            self.reply(200, {"defaults": {}, "overrides": {"prefix": f"catalogs/{CATALOG}"}})
        elif method == "GET" and REST_NAMESPACES.match(path):
            self.reply(200, {"namespaces": [] if "parent=" in url.query else [[SCHEMA]]})
        elif method == "GET" and (match := REST_TABLES.match(path)):
            with LOCK:
                names = sorted(TABLES) if match.group("namespace") == SCHEMA else []
            self.reply(200, {"identifiers": [{"namespace": [SCHEMA], "name": name} for name in names]})
        elif method in ("GET", "DELETE") and (match := REST_TABLE.match(path)):
            table_name = match.group("table")
            with LOCK:
                exists = match.group("namespace") == SCHEMA and table_name in TABLES
                if exists and method == "DELETE":
                    del TABLES[table_name]
            if not exists:
                self.not_found("NoSuchTableException", f"Table does not exist: {SCHEMA}.{table_name}")
            elif method == "DELETE":
                self.reply(204)
            else:
                self.reply(200, table_metadata(table_name))
        else:
            self.reply(404, {"error": {"message": f"Unexpected {method} {path}", "type": "NotFoundException", "code": 404}})

    def do_GET(self):
        self.handle_request("GET")

    def do_POST(self):
        self.handle_request("POST")

    def do_DELETE(self):
        self.handle_request("DELETE")

    def do_HEAD(self):
        self.handle_request("HEAD")

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), RequestHandler).serve_forever()
