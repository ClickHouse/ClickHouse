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

# Describe this hop rather than the request, so they must not be forwarded.
HOP_BY_HOP_HEADERS = {"host", "content-length", "accept-encoding", "connection"}


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

    def _health_check(self):
        response = b"OK"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def _proxy(self, method):
        if self.path == "/":
            self._health_check()
            return

        path = self.path.replace("/iceberg-rest/", "/iceberg/")

        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None

        request = urllib.request.Request(UPSTREAM + path, data=body, method=method)
        for name, value in self.headers.items():
            if name.lower() not in HOP_BY_HOP_HEADERS:
                request.add_header(name, value)

        try:
            response = urllib.request.urlopen(request)
            status, data = response.status, response.read()
            content_type = response.headers.get("Content-Type", "application/json")
        except urllib.error.HTTPError as e:
            status, data = e.code, e.read()
            content_type = e.headers.get("Content-Type", "application/json")

        if status == 200:
            if "/unity-catalog/tables" in path:
                data = patch_tables_response(data)
            elif "/unity-catalog/iceberg/" in path:
                data = normalize_scheme(data)

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._proxy("GET")

    def do_POST(self):
        self._proxy("POST")

    def do_HEAD(self):
        self._proxy("HEAD")

    def log_message(self, fmt, *args):
        print("%s %s" % (self.command, self.path), flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
