#!/usr/bin/env python3
"""Makes the open-source Unity Catalog server look like Databricks to
`UnifiedUnityCatalog`, for the tables in `UNIFORM_TABLES` only:

- serves the Iceberg REST catalog at `/iceberg-rest`, not `/iceberg`;
- reports an Iceberg `securable_kind`, which the open-source server never sends;
- writes the table location as `file:///tmp/...`, which `setLocation` requires.
"""
import json
import re
import sys
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = "http://localhost:8080"

# The only UniForm table in the seeded sample data.
UNIFORM_TABLES = {"marksheet_uniform"}

ICEBERG_SECURABLE_KIND = "TABLE_DELTA_ICEBERG_EXTERNAL"

# The lookahead makes this a no-op on an already-well-formed `file:///tmp/...`.
SINGLE_SLASH_SCHEME = re.compile(r"file:/(?=tmp/marksheet_uniform)")

def normalize_scheme(data):
    return SINGLE_SLASH_SCHEME.sub("file:///", data.decode()).encode()


def patch_table(table):
    if table.get("name") in UNIFORM_TABLES:
        table["securable_kind"] = ICEBERG_SECURABLE_KIND
    return table


def patch_tables_response(data):
    """The catalog uses both the paged listing and the single-table response."""
    body = json.loads(data)
    if "tables" in body:
        body["tables"] = [patch_table(t) for t in body["tables"]]
    elif "name" in body:
        body = patch_table(body)
    return json.dumps(body).encode()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path == "/":
            self._reply(200, b"OK")
            return

        path = self.path.replace("/iceberg-rest/", "/iceberg/")
        try:
            response = urllib.request.urlopen(UPSTREAM + path)
            status, data = response.status, response.read()
        except urllib.error.HTTPError as e:
            status, data = e.code, e.read()

        if status == 200:
            if "/unity-catalog/tables" in path:
                data = patch_tables_response(data)
            elif "/unity-catalog/iceberg/" in path:
                data = normalize_scheme(data)

        self._reply(status, data)

    def _reply(self, status, data):
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        print("%s %s" % (self.command, self.path), flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
