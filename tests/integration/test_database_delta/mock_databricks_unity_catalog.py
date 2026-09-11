#!/usr/bin/env python3
"""
Mock of the Databricks Unity Catalog REST API.

Open source Unity Catalog cannot create Databricks-managed streaming tables
(`securable_kind` = `TABLE_STREAMING_LIVE_TABLE`) or materialized views
(`TABLE_MATERIALIZED_VIEW`), whose Delta data lives at
`properties.spark.internal.*.backing_table_path` instead of
`storage_location`. This server reproduces exactly that metadata shape, while
the backing Delta tables themselves are real tables written by Spark.

Usage: mock_databricks_unity_catalog.py <config.json> <port>

The config file contains the catalog layout:
{
    "schema": "<schema name>",
    "tables": [
        {"name": "<table name>", "metadata": {<full response of GET /tables/{full_name}>}},
        ...
    ]
}
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

API_PREFIX = "/api/2.1/unity-catalog"

with open(sys.argv[1]) as f:
    CONFIG = json.load(f)
PORT = int(sys.argv[2])


class MockUnityCatalogHandler(BaseHTTPRequestHandler):
    def _send_json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == f"{API_PREFIX}/schemas":
            self._send_json(
                {
                    "schemas": [
                        {
                            "name": CONFIG["schema"],
                            "catalog_name": "unity",
                            "full_name": f"unity.{CONFIG['schema']}",
                        }
                    ]
                }
            )
            return

        if path == f"{API_PREFIX}/tables":
            self._send_json({"tables": [{"name": t["name"]} for t in CONFIG["tables"]]})
            return

        for table in CONFIG["tables"]:
            if path == f"{API_PREFIX}/tables/unity.{CONFIG['schema']}.{table['name']}":
                self._send_json(table["metadata"])
                return

        self._send_json({"error": f"unexpected path {path}"}, status=404)

    def do_POST(self):
        path = self.path.split("?")[0]
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)

        # Not expected to be called for file:// locations, but respond
        # gracefully instead of resetting the connection.
        if path == f"{API_PREFIX}/temporary-table-credentials":
            self._send_json({})
            return

        self._send_json({"error": f"unexpected path {path}"}, status=404)

    def log_message(self, format, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))
        sys.stderr.flush()


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", PORT), MockUnityCatalogHandler).serve_forever()
