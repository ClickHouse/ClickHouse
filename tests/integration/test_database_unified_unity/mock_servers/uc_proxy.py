#!/usr/bin/env python3
"""Proxy in front of the open-source Unity Catalog server that emulates the two
Databricks-specific behaviours `UnifiedUnityCatalog` depends on:

1. Databricks serves the Iceberg REST catalog at `{base}/iceberg-rest`, the
   open-source server at `{base}/iceberg`. Requests are rewritten.
2. Databricks reports a managed Iceberg table with `data_source_format = DELTA`
   plus a `securable_kind` of `TABLE_DELTA_ICEBERG_*`. The open-source server
   never sends `securable_kind` and cannot register an Iceberg table at all, so
   the kind is injected into the response for the UniForm tables listed below.

It also repairs a defect in the server's own seeded sample data: the UniForm
table is registered at `file:/tmp/marksheet_uniform`, a location that is both
outside `user_files` and, in the Iceberg metadata, missing the `//` that
`TableMetadata::setLocation` requires. Responses are rewritten to the directory
the data really lives in. The metadata file on disk is left alone on purpose:
`IcebergPathResolver` takes the table location from there and uses it to rebase
the `file:/tmp/...` paths embedded in the manifests onto the real directory.

Only the tables in `UNIFORM_TABLES` are patched, so a database pointed at this
proxy still sees every other table exactly as the upstream server reports it.
"""
import json
import re
import sys
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = "http://localhost:8080"

# Tables the proxy presents as managed Iceberg. `marksheet_uniform` is the only
# UniForm table in the server's seeded sample data.
UNIFORM_TABLES = {"marksheet_uniform"}

ICEBERG_SECURABLE_KIND = "TABLE_DELTA_ICEBERG_EXTERNAL"

# Where `start_unity_catalog` copies the server, and where the UniForm table's
# data really is. The seeded registration points at /tmp instead.
UC_ROOT = "/var/lib/clickhouse/user_files/unitycatalog"
UNIFORM_DIR = UC_ROOT + "/etc/data/external/unity/default/tables/marksheet_uniform"

# Matches both the `file:///tmp/...` of the tables API and the `file:/tmp/...`
# of the Iceberg metadata. Anchored on the table name, so nothing else can match.
STALE_LOCATION = re.compile(r"file:/{1,3}tmp/marksheet_uniform")

# Headers that describe this hop rather than the request, so they must not be forwarded.
HOP_BY_HOP_HEADERS = {"host", "content-length", "accept-encoding", "connection"}


def repair_locations(data):
    return STALE_LOCATION.sub("file://" + UNIFORM_DIR, data.decode()).encode()


def patch_table(table):
    if table.get("name") in UNIFORM_TABLES:
        table["securable_kind"] = ICEBERG_SECURABLE_KIND
    return table


def patch_tables_response(data):
    """Handles both the paged listing (`{"tables": [...]}`) and the
    single-table response (`{"name": ...}`); the catalog uses both."""
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
                data = patch_tables_response(repair_locations(data))
            elif "/unity-catalog/iceberg/" in path:
                data = repair_locations(data)

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

    def do_DELETE(self):
        self._proxy("DELETE")

    def log_message(self, fmt, *args):
        print("%s %s" % (self.command, self.path), flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
