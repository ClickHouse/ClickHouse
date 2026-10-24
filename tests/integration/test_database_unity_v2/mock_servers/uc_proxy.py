#!/usr/bin/env python3
"""Makes the open-source Unity Catalog server look like Databricks to
`UnityV2Catalog`, for the tables in `UNIFORM_TABLES` only:

- serves the Iceberg REST catalog at `/iceberg-rest`, not `/iceberg`;
- reports an Iceberg `securable_kind`, which the open-source server never sends;
- writes the table location as `file:///tmp/...`, which `setLocation` requires.

For the tables in `PATCHED_TABLE_TYPES` it reports a `table_type` other than
`EXTERNAL` (or none at all), which the open-source server has no notion of.

It also serves `managed_delta` under `ESCAPED_NAME`, a name the open-source server
would not accept, and gives `REPLACED_TABLE` a new `table_id` on every lookup.

It also emulates Databricks authentication:

- `POST /oidc/v1/token` mints OAuth tokens for `CLIENT_ID:CLIENT_SECRET`, and
  requires `scope=all-apis` as Databricks service principals do;
- every proxied route requires `Authorization: Bearer <token>`, where the token
  is either `PAT_TOKEN` or a minted OAuth token, and replies 401 otherwise;
- `/control/*` endpoints let the test expire tokens.

It vends the MinIO credentials from the command line on
`POST temporary-table-credentials`, and records the requested `operation`.
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

# Tables reported as not external, name -> `table_type`, where None removes the field. A static
# mapping, so no test can change what another test sees. The legacy resolver ignores `table_type`
# entirely, which is why these values have to be refused by the write guard rather than by the read rule.
PATCHED_TABLE_TYPES = {
    "managed_delta": "MANAGED",
    "clone_delta": "MANAGED_SHALLOW_CLONE",
    "foreign_delta": "FOREIGN",
    "untyped_delta": None,
}

# The open-source server allows only `[a-zA-Z0-9_@-]` in a name. In a request path this one
# is split by `?`, `#` and `/`, changed by `%41`, and rejected for `é` unless it is escaped.
ESCAPED_NAME = "a?b#c%41/é"
ESCAPED_NAME_TARGET = "managed_delta"
TABLE_ROUTE = "/unity-catalog/tables/"

# As if another table took this name after each lookup of it.
REPLACED_TABLE = "replaced_delta"

# The lookahead makes this a no-op on an already-well-formed `file:///tmp/...`.
SINGLE_SLASH_SCHEME = re.compile(r"file:/(?=tmp/marksheet_uniform)")

# Must match the constants in test.py.
CLIENT_ID = "test-client"
CLIENT_SECRET = "test-secret"
PAT_TOKEN = "dapi-test-pat"

# Mutated from handler threads; `set` operations are atomic under the GIL.
VALID_TOKENS = set()
PAT_VALID = True
# Principal can use the Unity tables API but not the Iceberg REST catalog.
ICEBERG_REST_DENIED = False
ICEBERG_REST_DENIED_BODY = (
    b'{"error_code": "PERMISSION_DENIED", '
    b'"message": "External data access is not enabled on the metastore"}'
)

TEMPORARY_CREDENTIALS_PATH = "/api/2.1/unity-catalog/temporary-table-credentials"
STORAGE_ACCESS_KEY = ""
STORAGE_SECRET_KEY = ""
CREDENTIAL_OPERATIONS = []


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
    name = table.get("name")
    if name in UNIFORM_TABLES:
        table["securable_kind"] = ICEBERG_SECURABLE_KIND
    if name in PATCHED_TABLE_TYPES:
        if PATCHED_TABLE_TYPES[name] is None:
            table.pop("table_type", None)
        else:
            table["table_type"] = PATCHED_TABLE_TYPES[name]
    if name == REPLACED_TABLE:
        table["table_id"] = str(uuid.uuid4())
    return table


def patch_tables_response(data, rename_to=None):
    """The catalog uses both the paged listing and the single-table response."""
    body = json.loads(data)
    if "tables" in body:
        body["tables"] = [patch_table(t) for t in body["tables"]]
    elif "name" in body:
        body = patch_table(body)
        if rename_to is not None:
            body["name"] = rename_to
    return json.dumps(body).encode()


def resolve_escaped_name(path):
    """The upstream path for a request that names `ESCAPED_NAME` as one escaped path
    segment, or None."""
    prefix, route, segment = path.partition(TABLE_ROUTE)
    if not route or "/" in segment or "?" in segment:
        return None
    schema, _, name = urllib.parse.unquote(segment).rpartition(".")
    if name != ESCAPED_NAME:
        return None
    return f"{prefix}{TABLE_ROUTE}{schema}.{ESCAPED_NAME_TARGET}"


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

        if ICEBERG_REST_DENIED and "/iceberg-rest/" in self.path:
            self._reply(403, ICEBERG_REST_DENIED_BODY)
            return

        path = self.path.replace("/iceberg-rest/", "/iceberg/")
        escaped_path = resolve_escaped_name(path)
        if escaped_path is not None:
            path = escaped_path
        try:
            response = urllib.request.urlopen(UPSTREAM + path)
            status, data = response.status, response.read()
        except urllib.error.HTTPError as e:
            status, data = e.code, e.read()

        if status == 200:
            if "/unity-catalog/tables" in path:
                rename_to = ESCAPED_NAME if escaped_path is not None else None
                data = patch_tables_response(data, rename_to)
            elif "/unity-catalog/iceberg/" in path:
                data = normalize_scheme(data)

        self._reply(status, data)

    def do_POST(self):
        if self.path == "/oidc/v1/token":
            self._handle_token_request()
            return

        if self.path == TEMPORARY_CREDENTIALS_PATH:
            self._handle_temporary_credentials_request()
            return

        self._reply(404, b'{"error": "unsupported POST route"}')

    def _handle_temporary_credentials_request(self):
        if not self._check_auth():
            return
        request = json.loads(self._read_body())
        CREDENTIAL_OPERATIONS.append(request["operation"])
        body = {
            "aws_temp_credentials": {
                "access_key_id": STORAGE_ACCESS_KEY,
                "secret_access_key": STORAGE_SECRET_KEY,
                "session_token": "",
            }
        }
        self._reply(200, json.dumps(body).encode())

    def _handle_control(self):
        global PAT_VALID, ICEBERG_REST_DENIED
        if self.path == "/control/expire":
            VALID_TOKENS.clear()
            self._reply(200, b"OK")
        elif self.path == "/control/revoke_pat":
            PAT_VALID = False
            self._reply(200, b"OK")
        elif self.path == "/control/restore_pat":
            PAT_VALID = True
            self._reply(200, b"OK")
        elif self.path == "/control/deny_iceberg_rest":
            ICEBERG_REST_DENIED = True
            self._reply(200, b"OK")
        elif self.path == "/control/allow_iceberg_rest":
            ICEBERG_REST_DENIED = False
            self._reply(200, b"OK")
        elif self.path == "/control/credential_operations":
            self._reply(200, json.dumps(CREDENTIAL_OPERATIONS).encode())
        elif self.path == "/control/reset_credential_operations":
            CREDENTIAL_OPERATIONS.clear()
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
    STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY = sys.argv[2], sys.argv[3]
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
