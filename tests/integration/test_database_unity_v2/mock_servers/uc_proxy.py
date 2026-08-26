#!/usr/bin/env python3
"""Makes the open-source Unity Catalog server look like Databricks to
`UnityV2Catalog`, for the tables in `UNIFORM_TABLES` only:

- serves the Iceberg REST catalog at `/iceberg-rest`, not `/iceberg`;
- reports an Iceberg `securable_kind`, which the open-source server never sends;
- writes the table location as `file:///tmp/...`, which `setLocation` requires.

It also emulates Databricks authentication:

- `POST /oidc/v1/token` mints OAuth tokens for `CLIENT_ID:CLIENT_SECRET`, and
  requires `scope=all-apis` as Databricks service principals do;
- every proxied route requires `Authorization: Bearer <token>`, where the token
  is either `PAT_TOKEN` or a minted OAuth token, and replies 401 otherwise;
- `/control/*` endpoints let the test expire tokens.
"""
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = "http://localhost:8080"

# The only UniForm table in the seeded sample data.
UNIFORM_TABLES = {"marksheet_uniform"}

ICEBERG_SECURABLE_KIND = "TABLE_DELTA_ICEBERG_EXTERNAL"

# The lookahead makes this a no-op on an already-well-formed `file:///tmp/...`.
SINGLE_SLASH_SCHEME = re.compile(r"file:/(?=tmp/marksheet_uniform)")

# Must match the constants in test.py.
CLIENT_ID = "test-client"
CLIENT_SECRET = "test-secret"
PAT_TOKEN = "dapi-test-pat"

# Mutated from handler threads; `set` operations are atomic under the GIL.
VALID_TOKENS = set()
PAT_VALID = True


def is_authorized(header):
    if not header or not header.startswith("Bearer "):
        return False
    token = header[len("Bearer "):]
    if token == PAT_TOKEN:
        return PAT_VALID
    return token in VALID_TOKENS


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

        if self.path.startswith("/control/"):
            self._handle_control()
            return

        if not self._check_auth():
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

    def do_POST(self):
        if self.path == "/oidc/v1/token":
            self._handle_token_request()
            return

        self._reply(404, b'{"error": "unsupported POST route"}')

    def _handle_control(self):
        global PAT_VALID
        if self.path == "/control/expire":
            VALID_TOKENS.clear()
            self._reply(200, b"OK")
        elif self.path == "/control/revoke_pat":
            PAT_VALID = False
            self._reply(200, b"OK")
        elif self.path == "/control/restore_pat":
            PAT_VALID = True
            self._reply(200, b"OK")
        else:
            self._reply(404, b'{"error": "unknown control route"}')

    def _read_body(self):
        """ClickHouse sends request bodies chunked, which `BaseHTTPRequestHandler`
        does not decode."""
        if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
            return self.rfile.read(int(self.headers.get("Content-Length", 0)))

        chunks = []
        while True:
            size = int(self.rfile.readline().strip(), 16)
            if size == 0:
                self.rfile.readline()
                return b"".join(chunks)
            chunks.append(self.rfile.read(size))
            self.rfile.readline()

    def _handle_token_request(self):
        """The RFC 6749 client-credentials grant: parameters in the request body."""
        params = urllib.parse.parse_qs(self._read_body().decode())

        if (
            params.get("grant_type") != ["client_credentials"]
            or params.get("client_id") != [CLIENT_ID]
            or params.get("client_secret") != [CLIENT_SECRET]
        ):
            self._reply(401, b'{"error": "invalid_client"}')
            return

        if params.get("scope") != ["all-apis"]:
            self._reply(400, b'{"error": "invalid_scope"}')
            return

        token = uuid.uuid4().hex
        VALID_TOKENS.add(token)
        body = {
            "access_token": token,
            "token_type": "bearer",
            "expires_in": 3600,
        }
        self._reply(200, json.dumps(body).encode())

    def _check_auth(self):
        if is_authorized(self.headers.get("Authorization")):
            return True
        self._reply(401, b'{"error": "invalid or expired token"}')
        return False

    def _reply(self, status, data):
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        print("%s %s" % (self.command, self.path), flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
