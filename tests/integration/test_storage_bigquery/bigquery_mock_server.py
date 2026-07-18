#!/usr/bin/env python3
"""A mock of the Google BigQuery v2 REST API, sufficient for the `bigquery`
table function / `BigQuery` table engine integration tests:
  - tables.get       GET  /bigquery/v2/projects/{p}/datasets/{d}/tables/{t}
  - tabledata.list   GET  /bigquery/v2/projects/{p}/datasets/{d}/tables/{t}/data
  - insertAll        POST /bigquery/v2/projects/{p}/datasets/{d}/tables/{t}/insertAll
  - OAuth 2.0 token  POST /token (refresh_token and jwt-bearer grants)
  - control          GET /__stats__, GET /__reset__

Rows are stored in the wire format of tabledata.list ("f"/"v" cells).
TIMESTAMP values are served as int64 microseconds, as the client always sends
formatOptions.useInt64Timestamp=true.
"""

import base64
import datetime
import http.server
import json
import re
import sys
import urllib.parse

PROJECT = "test-project"
DATASET = "test_dataset"

ACCESS_TOKEN = "test-static-token"
SA_TOKEN = "test-sa-token"
ADC_TOKEN = "test-adc-token"

SA_CLIENT_EMAIL = "tester@example-project.iam.gserviceaccount.com"
ADC_CLIENT_ID = "test-client-id.apps.googleusercontent.com"
ADC_CLIENT_SECRET = "test-client-secret"
ADC_REFRESH_TOKEN = "test-refresh-token"

BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery"

# The server enforces its own page size cap, like the real API does (by response size).
SERVER_PAGE_CAP = 4


def f(name, type_, mode="NULLABLE", fields=None, precision=None, scale=None):
    field = {"name": name, "type": type_, "mode": mode}
    if fields:
        field["fields"] = fields
    if precision is not None:
        field["precision"] = str(precision)
        field["scale"] = str(scale or 0)
    return field


def v(value):
    return {"v": value}


def row(*values):
    return {"f": [v(x) for x in values]}


TYPES_SCHEMA = [
    f("i", "INTEGER", "REQUIRED"),
    f("fl", "FLOAT"),
    f("s", "STRING"),
    f("bin", "BYTES"),
    f("flag", "BOOLEAN"),
    f("d", "DATE"),
    f("t", "TIME"),
    f("dt", "DATETIME"),
    f("ts", "TIMESTAMP"),
    f("num", "NUMERIC"),
    f("bignum", "BIGNUMERIC"),
    f("num_p", "NUMERIC", precision=10, scale=2),
    f("geo", "GEOGRAPHY"),
    f("j", "JSON"),
    f("arr", "INTEGER", "REPEATED"),
    f(
        "rec",
        "RECORD",
        "NULLABLE",
        fields=[
            f("x", "INTEGER"),
            f("y", "STRING", "REQUIRED"),
            f("tags", "STRING", "REPEATED"),
        ],
    ),
    f(
        "recs",
        "RECORD",
        "REPEATED",
        fields=[
            f("k", "INTEGER", "REQUIRED"),
            f("val", "STRING"),
        ],
    ),
]

TYPES_ROWS = [
    row(
        "1",
        "1.5",
        "hello",
        base64.b64encode(b"binary-data").decode(),
        "true",
        "2024-01-02",
        "03:04:05.123456",
        "2024-01-02T03:04:05.123456",
        "1704164645123456",
        "12345.123456789",
        "1234567890.12345678901234567890123456789012345678",
        "12345678.99",
        "POINT(1 2)",
        '{"a":1}',
        [v("1"), v("2"), v("3")],
        {"f": [v("7"), v("seven"), v([v("t1"), v("t2")])]},
        [{"v": {"f": [v("1"), v("one")]}}, {"v": {"f": [v("2"), v(None)]}}],
    ),
    row(
        "2",
        "Infinity",
        "",
        None,
        "false",
        None,
        None,
        None,
        None,
        "-0.000000001",
        None,
        None,
        None,
        None,
        None,
        None,
        [],
    ),
    row(
        "3",
        "NaN",
        None,
        base64.b64encode("привет".encode()).decode(),
        None,
        "1970-01-01",
        "23:59:59",
        "2299-12-31T23:59:59",
        "0",
        None,
        "-99999999999999999999999999999999999999.99999999999999999999999999999999999999",
        "0.01",
        None,
        "[1,2,3]",
        [v("-1")],
        {"f": [v(None), v("y-only"), v(None)]},
        [],
    ),
]

PAGING_SCHEMA = [
    f("i", "INTEGER", "REQUIRED"),
    f("s", "STRING"),
]

PAGING_ROWS = [row(str(i), "value" + str(i)) for i in range(10)]

WRITABLE_SCHEMA = [
    f("id", "INTEGER", "REQUIRED"),
    f("name", "STRING"),
    f("fl", "FLOAT"),
    f("ok", "BOOLEAN"),
    f("d", "DATE"),
    f("ts", "TIMESTAMP"),
    f("num", "NUMERIC"),
    f("bin", "BYTES"),
    f("tags", "STRING", "REPEATED"),
    f("meta", "RECORD", "NULLABLE", fields=[f("a", "INTEGER")]),
]

# A malformed fixture: a REPEATED field whose response carries {"v": null} elements. A real BigQuery
# table cannot store NULL array elements (ARRAY<T> is equivalent to ARRAY<T NOT NULL>), so such a payload
# is malformed input and the reader rejects it instead of coercing the element to a default.
ARR_NULLS_SCHEMA = [
    f("i", "INTEGER", "REQUIRED"),
    f("arr", "INTEGER", "REPEATED"),
    f("tags", "STRING", "REPEATED"),
]

ARR_NULLS_ROWS = [
    row("1", [v("1"), v(None), v("2")], [v("a"), v(None)]),
]

# Nested RECORDs with different nullability at the outer and inner levels: the outer record is
# NULLABLE (inferred as Nullable(Tuple(...))) and the inner record is REQUIRED (inferred as a plain
# Tuple(...)). This guards the schema-compatibility check against accepting a declaration that moves
# the Nullable to the inner record - Tuple(child Nullable(Tuple(x Int64))) - which encodes different
# NULL states than the inferred Nullable(Tuple(child Tuple(x Int64))).
NESTED_REC_SCHEMA = [
    f("i", "INTEGER", "REQUIRED"),
    f(
        "parent",
        "RECORD",
        "NULLABLE",
        fields=[
            f(
                "child",
                "RECORD",
                "REQUIRED",
                fields=[f("x", "INTEGER", "REQUIRED")],
            ),
        ],
    ),
]

NESTED_REC_ROWS = [
    row("1", {"f": [v({"f": [v("5")]})]}),
    row("2", None),
]

# A RANGE column is exposed as a read-only String; tabledata.list serves it as the formatted range text.
RANGE_SCHEMA = [
    f("i", "INTEGER", "REQUIRED"),
    f("r", "RANGE"),
]

RANGE_ROWS = [
    row("1", "[2020-01-01, 2020-12-31)"),
    row("2", "[2021-01-01, UNBOUNDED)"),
]

# A deliberately wide table. BigQuery allows up to 10000 columns with names up to 300 bytes, so the
# comma-separated `selectedFields` list for a wide `SELECT *` can exceed the request-URL length limit.
# The reader must then fall back to an empty `selectedFields` (which asks BigQuery for all columns).
# The column-name generation here must match test.py::wide_column_name / WIDE_COLUMN_COUNT.
WIDE_COLUMN_COUNT = 40


def wide_column_name(i):
    return "wide_column_" + str(i).zfill(4) + "_" + "x" * 280


WIDE_SCHEMA = [f("i", "INTEGER", "REQUIRED")] + [
    f(wide_column_name(i), "INTEGER") for i in range(WIDE_COLUMN_COUNT)
]

WIDE_ROWS = [row(*(["1"] * (WIDE_COLUMN_COUNT + 1)))]

TABLES = {}


def reset_tables():
    global TABLES
    TABLES = {
        "test_types": {
            "type": "TABLE",
            "schema": TYPES_SCHEMA,
            "rows": [json.loads(json.dumps(r)) for r in TYPES_ROWS],
        },
        "test_paging": {
            "type": "TABLE",
            "schema": PAGING_SCHEMA,
            "rows": [json.loads(json.dumps(r)) for r in PAGING_ROWS],
        },
        "writable": {"type": "TABLE", "schema": WRITABLE_SCHEMA, "rows": []},
        "a_view": {"type": "VIEW", "schema": PAGING_SCHEMA, "rows": []},
        "a_matview": {
            "type": "MATERIALIZED_VIEW",
            "schema": PAGING_SCHEMA,
            "rows": [],
        },
        "test_arr_nulls": {
            "type": "TABLE",
            "schema": ARR_NULLS_SCHEMA,
            "rows": [json.loads(json.dumps(r)) for r in ARR_NULLS_ROWS],
        },
        "test_nested_rec": {
            "type": "TABLE",
            "schema": NESTED_REC_SCHEMA,
            "rows": [json.loads(json.dumps(r)) for r in NESTED_REC_ROWS],
        },
        "test_range": {
            "type": "TABLE",
            "schema": RANGE_SCHEMA,
            "rows": [json.loads(json.dumps(r)) for r in RANGE_ROWS],
        },
        "test_wide": {
            "type": "TABLE",
            "schema": WIDE_SCHEMA,
            "rows": [json.loads(json.dumps(r)) for r in WIDE_ROWS],
        },
    }


STATS = {
    "data_requests": [],
    "insert_requests": [],
    "token_requests": [],
    "schema_requests": [],
}
FAIL_INSERTS = [False]
# When set to an integer N, an insertAll request is rejected once the table already holds >= N rows.
# Used to simulate a mid-INSERT failure that leaves an earlier committed prefix behind.
FAIL_INSERTS_AFTER = [None]
# The set of `id` values whose row is rejected with a per-row "invalid" error while the other rows of
# the same insertAll request still commit. This models BigQuery's partial success within a single
# response (rows listed in insertErrors are rejected, the rest are inserted), as opposed to the
# all-or-nothing schema-mismatch ("stopped") case.
REJECT_ROW_IDS = set()


def google_error(code, status, message):
    return code, {"error": {"code": code, "message": message, "status": status}}


def b64url_decode(data):
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def convert_insert_value(field, value):
    """Convert a value of an insertAll "json" row into the tabledata.list wire format."""
    if value is None:
        return None
    if field["mode"] == "REPEATED":
        if not isinstance(value, list):
            raise ValueError(f"field {field['name']}: expected an array")
        elem = dict(field, mode="NULLABLE")
        return [{"v": convert_insert_value(elem, x)} for x in value]

    type_ = field["type"]
    if type_ == "RECORD":
        if not isinstance(value, dict):
            raise ValueError(f"field {field['name']}: expected an object")
        unknown = set(value) - {child["name"] for child in field["fields"]}
        if unknown:
            raise ValueError(
                f"field {field['name']}: unknown sub-fields {sorted(unknown)}"
            )
        return {
            "f": [
                {"v": convert_insert_value(child, value.get(child["name"]))}
                for child in field["fields"]
            ]
        }
    if type_ == "TIMESTAMP":
        # Accept the civil UTC format the client sends, and a numeric epoch.
        if isinstance(value, str):
            m = re.fullmatch(
                r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(?: UTC)?",
                value,
            )
            if not m:
                raise ValueError(
                    f"field {field['name']}: cannot parse timestamp {value!r}"
                )
            micros = int(
                datetime.datetime(
                    *(int(m.group(i)) for i in range(1, 7)),
                    tzinfo=datetime.timezone.utc,
                ).timestamp()
            ) * 1000000 + int((m.group(7) or "0").ljust(6, "0"))
            return str(micros)
        return str(int(float(value) * 1000000))
    if type_ == "BOOLEAN":
        if not isinstance(value, bool):
            raise ValueError(f"field {field['name']}: expected a boolean")
        return "true" if value else "false"
    if type_ == "INTEGER":
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ValueError(f"field {field['name']}: expected an integer")
        return str(int(value))
    if type_ == "FLOAT":
        if isinstance(value, str):
            if value not in ("NaN", "Infinity", "-Infinity"):
                raise ValueError(
                    f"field {field['name']}: unexpected float string {value!r}"
                )
            return value
        return repr(float(value))
    if type_ == "BYTES":
        base64.b64decode(value)  # validate
        return value
    # STRING, DATE, TIME, DATETIME, NUMERIC, BIGNUMERIC, GEOGRAPHY, JSON pass through as text.
    return str(value)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, pattern, *args):
        sys.stderr.write(pattern % args + "\n")

    def send_json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def check_auth(self):
        auth = self.headers.get("Authorization", "")
        if auth in (
            f"Bearer {ACCESS_TOKEN}",
            f"Bearer {SA_TOKEN}",
            f"Bearer {ADC_TOKEN}",
        ):
            return True
        self.send_json(
            *google_error(
                401,
                "UNAUTHENTICATED",
                "Request had invalid authentication credentials.",
            )
        )
        return False

    def get_table(self, path):
        m = re.fullmatch(
            rf"/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/([^/]+)(/data|/insertAll)?",
            path,
        )
        if not m:
            self.send_json(*google_error(404, "NOT_FOUND", f"Unexpected path {path}"))
            return None, None
        table_name = urllib.parse.unquote(m.group(1))
        if table_name not in TABLES:
            self.send_json(
                *google_error(
                    404,
                    "NOT_FOUND",
                    f"Not found: Table {PROJECT}:{DATASET}.{table_name}",
                )
            )
            return None, None
        return TABLES[table_name], m.group(2)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = dict(urllib.parse.parse_qsl(parsed.query))

        if parsed.path == "/":
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/__stats__":
            self.send_json(200, STATS)
            return
        if parsed.path == "/__reset__":
            reset_tables()
            STATS["data_requests"].clear()
            STATS["insert_requests"].clear()
            STATS["token_requests"].clear()
            STATS["schema_requests"].clear()
            FAIL_INSERTS[0] = False
            FAIL_INSERTS_AFTER[0] = None
            REJECT_ROW_IDS.clear()
            self.send_json(200, {})
            return
        if parsed.path == "/__fail_inserts__":
            FAIL_INSERTS[0] = True
            self.send_json(200, {})
            return
        if parsed.path == "/__fail_inserts_after__":
            rows = params.get("rows")
            FAIL_INSERTS_AFTER[0] = int(rows) if rows is not None else None
            self.send_json(200, {})
            return
        if parsed.path == "/__reject_row_ids__":
            ids = params.get("ids")
            REJECT_ROW_IDS.clear()
            if ids:
                REJECT_ROW_IDS.update(ids.split(","))
            self.send_json(200, {})
            return
        if not self.check_auth():
            return

        table, suffix = self.get_table(parsed.path)
        if table is None:
            return

        if suffix is None:
            STATS["schema_requests"].append({"path": parsed.path})
            self.send_json(
                200,
                {
                    "type": table["type"],
                    "numRows": str(len(table["rows"])),
                    "schema": {"fields": table["schema"]},
                },
            )
            return

        if suffix == "/data":
            if table["type"] != "TABLE":
                self.send_json(
                    *google_error(
                        400,
                        "INVALID_ARGUMENT",
                        "Cannot list a table of type " + table["type"],
                    )
                )
                return
            if params.get("formatOptions.useInt64Timestamp") != "true":
                self.send_json(
                    *google_error(
                        400,
                        "INVALID_ARGUMENT",
                        "The test server requires formatOptions.useInt64Timestamp",
                    )
                )
                return
            STATS["data_requests"].append({"path": parsed.path, "params": params})

            rows = table["rows"]
            selected = params.get("selectedFields")
            if selected:
                names = selected.split(",")
                schema_names = [field["name"] for field in table["schema"]]
                unknown = set(names) - set(schema_names)
                if unknown:
                    self.send_json(
                        *google_error(
                            400,
                            "INVALID_ARGUMENT",
                            f"Unknown selected fields {sorted(unknown)}",
                        )
                    )
                    return
                # The real API returns the selected fields in the order of the table schema.
                indexes = [i for i, name in enumerate(schema_names) if name in names]
                rows = [{"f": [r["f"][i] for i in indexes]} for r in rows]

            start = int(params.get("pageToken", "0"))
            page_size = min(
                int(params.get("maxResults", str(SERVER_PAGE_CAP))), SERVER_PAGE_CAP
            )
            page_rows = rows[start : start + page_size]
            response = {"totalRows": str(len(rows)), "rows": page_rows}
            if start + page_size < len(rows):
                response["pageToken"] = str(start + page_size)
            if not page_rows:
                del response["rows"]
            self.send_json(200, response)
            return

        self.send_json(
            *google_error(400, "INVALID_ARGUMENT", f"Unexpected GET {parsed.path}")
        )

    def read_body(self):
        # ClickHouse sends POST bodies with chunked transfer encoding.
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            body = b""
            while True:
                size = int(self.rfile.readline().strip(), 16)
                if size == 0:
                    self.rfile.readline()
                    break
                body += self.rfile.read(size)
                self.rfile.readline()
            return body.decode()
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length).decode()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        body = self.read_body()

        if parsed.path == "/token":
            self.handle_token(body)
            return

        if not self.check_auth():
            return

        table, suffix = self.get_table(parsed.path)
        if table is None:
            return

        if suffix == "/insertAll":
            request = json.loads(body)
            rows = request.get("rows", [])
            STATS["insert_requests"].append(
                {
                    "path": parsed.path,
                    "rows": len(rows),
                    "body_bytes": len(body),
                    "insert_ids": [entry.get("insertId") for entry in rows],
                    # The raw "json" bodies as received on the wire, so tests can assert the JSON
                    # type of a value (e.g. that an INT64 is sent as a string, not a number). Skipped
                    # for very large requests to keep the /__stats__ response small.
                    "raw_rows": (
                        [entry.get("json", {}) for entry in rows]
                        if len(body) <= 1024 * 1024
                        else []
                    ),
                }
            )

            # BigQuery rejects insertId values longer than 128 characters.
            for entry in rows:
                insert_id = entry.get("insertId")
                if insert_id is not None and len(insert_id) > 128:
                    self.send_json(
                        *google_error(
                            400,
                            "INVALID_ARGUMENT",
                            f"insertId is too long: {len(insert_id)} characters (max 128)",
                        )
                    )
                    return

            # BigQuery's streaming API rejects requests larger than 10 MB.
            if len(body) > 10 * 1024 * 1024:
                self.send_json(
                    *google_error(
                        413,
                        "REQUEST_TOO_LARGE",
                        f"Request payload size exceeds the limit: {len(body)} bytes (max 10485760)",
                    )
                )
                return

            if FAIL_INSERTS[0]:
                errors = [
                    {
                        "index": i,
                        "errors": [
                            {"reason": "invalid", "message": "simulated failure"}
                        ],
                    }
                    for i in range(len(rows))
                ]
                self.send_json(200, {"insertErrors": errors})
                return

            # Simulate a mid-INSERT failure that keeps the already-committed prefix: once the table
            # already holds enough rows, reject this whole request (as BigQuery would a bad batch).
            if (
                FAIL_INSERTS_AFTER[0] is not None
                and len(table["rows"]) >= FAIL_INSERTS_AFTER[0]
            ):
                errors = [
                    {
                        "index": i,
                        "errors": [
                            {"reason": "invalid", "message": "simulated failure"}
                        ],
                    }
                    for i in range(len(rows))
                ]
                self.send_json(200, {"insertErrors": errors})
                return

            # Best-effort deduplication by insertId, mirroring BigQuery streaming inserts: a row whose
            # insertId was already committed to this table is dropped instead of inserted again.
            seen_insert_ids = table.setdefault("seen_insert_ids", set())

            errors = []
            converted = []
            committed_insert_ids = []
            # A schema mismatch (or a value that cannot be converted) stops the whole request: BigQuery
            # commits none of its rows. A configured per-row rejection instead rejects only that row and
            # still commits the rest, mirroring BigQuery's partial success within one insertAll response.
            stop_request = False
            for i, entry in enumerate(rows):
                insert_id = entry.get("insertId")
                if insert_id is not None and insert_id in seen_insert_ids:
                    continue
                data = entry.get("json", {})
                if REJECT_ROW_IDS and str(data.get("id")) in REJECT_ROW_IDS:
                    errors.append(
                        {
                            "index": i,
                            "errors": [
                                {
                                    "reason": "invalid",
                                    "message": "simulated per-row rejection",
                                }
                            ],
                        }
                    )
                    continue
                try:
                    unknown = set(data) - {field["name"] for field in table["schema"]}
                    if unknown:
                        raise ValueError(f"no such field: {sorted(unknown)}")
                    converted.append(
                        {
                            "f": [
                                {
                                    "v": convert_insert_value(
                                        field, data.get(field["name"])
                                    )
                                }
                                for field in table["schema"]
                            ]
                        }
                    )
                    if insert_id is not None:
                        committed_insert_ids.append(insert_id)
                except ValueError as e:
                    stop_request = True
                    errors.append(
                        {
                            "index": i,
                            "errors": [{"reason": "invalid", "message": str(e)}],
                        }
                    )
            if stop_request:
                # Schema mismatch ("stopped"): the whole request fails and none of the rows commit.
                self.send_json(200, {"insertErrors": errors})
                return
            # Commit the accepted rows (all of them when there were no per-row rejections), then report
            # any per-row rejections so a retry with the same insertIds deduplicates the committed rows.
            table["rows"].extend(converted)
            seen_insert_ids.update(committed_insert_ids)
            if errors:
                self.send_json(200, {"insertErrors": errors})
                return
            self.send_json(200, {"kind": "bigquery#tableDataInsertAllResponse"})
            return

        self.send_json(
            *google_error(400, "INVALID_ARGUMENT", f"Unexpected POST {parsed.path}")
        )

    def handle_token(self, body):
        params = dict(urllib.parse.parse_qsl(body))
        grant_type = params.get("grant_type", "")
        STATS["token_requests"].append({"grant_type": grant_type})

        if grant_type == "refresh_token":
            if (
                params.get("client_id") == ADC_CLIENT_ID
                and params.get("client_secret") == ADC_CLIENT_SECRET
                and params.get("refresh_token") == ADC_REFRESH_TOKEN
            ):
                self.send_json(
                    200,
                    {
                        "access_token": ADC_TOKEN,
                        "token_type": "Bearer",
                        "expires_in": 3600,
                    },
                )
            else:
                self.send_json(*google_error(401, "UNAUTHENTICATED", "invalid_grant"))
            return

        if grant_type == "urn:ietf:params:oauth:grant-type:jwt-bearer":
            try:
                header_b64, claims_b64, signature_b64 = params["assertion"].split(".")
                header = json.loads(b64url_decode(header_b64))
                claims = json.loads(b64url_decode(claims_b64))
                assert header["alg"] == "RS256", f"bad alg {header}"
                assert header["typ"] == "JWT", f"bad typ {header}"
                assert claims["iss"] == SA_CLIENT_EMAIL, f"bad iss {claims}"
                assert claims["scope"] == BIGQUERY_SCOPE, f"bad scope {claims}"
                assert claims["aud"].endswith("/token"), f"bad aud {claims}"
                assert claims["iat"] <= claims["exp"], f"bad iat/exp {claims}"
                assert len(b64url_decode(signature_b64)) >= 256, "bad signature length"
            except Exception as e:
                self.send_json(
                    *google_error(401, "UNAUTHENTICATED", f"invalid assertion: {e}")
                )
                return
            self.send_json(
                200,
                {"access_token": SA_TOKEN, "token_type": "Bearer", "expires_in": 3600},
            )
            return

        self.send_json(
            *google_error(
                400, "INVALID_ARGUMENT", f"unsupported grant_type {grant_type}"
            )
        )


if __name__ == "__main__":
    reset_tables()
    httpd = http.server.ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler)
    httpd.serve_forever()
