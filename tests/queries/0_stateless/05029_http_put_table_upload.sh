#!/usr/bin/env bash

set -euo pipefail

# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
TABLE="put_table_05029"
DOTTED_TABLE="${TABLE}.JSON"
NO_INSERT_USER="put_no_insert_05029_${DB}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS \`${NO_INSERT_USER}\`"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.\`${DOTTED_TABLE}\`"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.${TABLE}"
}
trap cleanup EXIT

cleanup
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.${TABLE} (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.\`${DOTTED_TABLE}\` (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "CREATE USER \`${NO_INSERT_USER}\` IDENTIFIED WITH no_password SETTINGS http_allow_database_as_path = 1, http_allow_table_as_file = 1"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${DB}.${TABLE} TO \`${NO_INSERT_USER}\`"

echo "===== PUT table upload ====="
echo "-- CSV format from the path"
printf '1,"one"\n2,"two"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.CSV"

echo "-- JSONEachRow format from the path"
printf '{"a":3,"b":"three"}\n' \
    | curl -sS -X PUT -H 'Content-Type: application/json' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.JSONEachRow"

echo "-- dotted table name with a separate format suffix"
printf '18,"dotted"\n' \
    | curl -fsS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${DOTTED_TABLE}.CSV" >/dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.\`${DOTTED_TABLE}\` WHERE a = 18 AND b = 'dotted'" | grep -qx '1'
echo "dotted-table-upload: OK"

echo "-- inserted rows"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB}.${TABLE} ORDER BY a"

echo "-- input_format overrides the path format"
printf '{"a":9,"b":"nine"}\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?input_format=JSONEachRow"

echo "-- format overrides the path format"
printf '{"a":10,"b":"ten"}\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?format=JSONEachRow"

echo "-- database-prefixed upload works with table-as-file disabled"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?http_allow_table_as_file=0"

echo "-- path filters are rejected for uploads"
printf '11,"filtered"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/a=1/${TABLE}.CSV?http_allow_filters_as_path=1" 2>&1 \
    | grep -oE "HTTP PUT table uploads do not support filters in the URL path"

echo "-- filtered row was not inserted"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE} WHERE a = 11"

echo "-- unqualified upload still requires table-as-file"
printf '5,"five"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${TABLE}.CSV?http_allow_database_as_path=1&http_allow_table_as_file=0" 2>&1 \
    | grep -oE "HTTP PUT table uploads require a table path with a known format"

echo "-- unqualified upload works with table-as-file enabled"
printf '5,"five"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${TABLE}.csv?database=${DB}&http_allow_database_as_path=0&http_allow_table_as_file=1"

echo "-- database-prefixed upload requires the database-path setting"
printf '6,"six"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?http_allow_database_as_path=0&http_allow_table_as_file=1" 2>&1 \
    | grep -oE "HTTP PUT table uploads require a table path with a known format"

echo "-- inserted rows after setting matrix"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB}.${TABLE} ORDER BY a"

echo "-- gzip compression suffix decompresses the request body"
printf '6,"six"\n' \
    | gzip -c \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.CSV.gz"

echo "-- matching Content-Encoding and path compression are accepted"
printf '7,"seven"\n' \
    | gzip -c \
    | curl -sS -X PUT -H 'Content-Type: text/csv' -H 'Content-Encoding: gzip' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV.gz"

echo "-- compression aliases are accepted"
printf '8,"eight"\n' \
    | gzip -c \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.CSV.gzip"

echo "-- conflicting Content-Encoding is rejected"
printf '9,"nine"\n' \
    | gzip -c \
    | curl -sS -X PUT -H 'Content-Type: text/csv' -H 'Content-Encoding: deflate' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV.gz" 2>&1 \
    | grep -oE 'Conflicting compression: .*Content-Encoding`'

echo "-- chunked PUT upload"
printf '13,"chunked"\n' \
    | curl --http1.1 -sS -X PUT -H 'Content-Type: text/csv' -H 'Transfer-Encoding: chunked' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV"

echo "-- Expect: 100-continue PUT upload"
printf '12,"expect"\n' \
    | curl --http1.1 -sS -X PUT -H 'Content-Type: text/csv' -H 'Expect: 100-continue' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV"

echo "-- deferred Expect: 100-continue PUT upload"
printf '16,"deferred-expect"\n' \
    | curl --http1.1 -sS -X PUT -H 'Content-Type: text/csv' -H 'Expect: 100-continue' \
        -H 'X-ClickHouse-100-Continue: defer' --expect100-timeout 300 --max-time 60 --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE} WHERE a = 16" | grep -qx '1'

echo "-- inserted rows after compressed and framed uploads"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB}.${TABLE} ORDER BY a"

echo "-- native-compressed PUT respects disabled checksum verification"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compress=1" \
    -d "SELECT concat('17,\"native-checksum\"', char(10)) FORMAT RawBLOB" \
    | python3 -c '
import sys

body = sys.stdin.buffer.read()
if len(body) <= 25:
    raise ValueError("Expected a native-compressed block")
sys.stdout.buffer.write(bytes(16) + body[16:])
' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?decompress=1&http_native_compression_disable_checksumming_on_decompress=1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE} WHERE a = 17 AND b = 'native-checksum'" | grep -qx '1'

echo "-- percent-encoded format suffixes are recognized"
printf '14,"encoded-dot"\n' \
    | curl --path-as-is -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}%2ECSV" >/dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE} WHERE a = 14" | grep -qx '1'

echo "-- malformed percent encoding in an intermediate path component returns 400"
python3 - <<PY
import socket


sock = socket.create_connection(("${CLICKHOUSE_HOST}", ${CLICKHOUSE_PORT_HTTP}), timeout=30)
try:
    sock.sendall(
        b"PUT /%ZZ/${TABLE}.CSV?http_allow_database_as_path=1 HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Length: 0\r\n"
        b"Connection: close\r\n\r\n"
    )
    response = sock.recv(4096)
finally:
    sock.close()

status = response.split(b" ", 2)[1].decode()
if status != "400":
    raise RuntimeError(f"expected HTTP 400, got HTTP {status}: {response!r}")
print("malformed-path-status:", status)
PY

echo "-- a compressed body that decodes to empty is rejected"
gzip -c </dev/null \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.CSV.gz" 2>&1 \
    | grep -oE "HTTP PUT table uploads require a non-empty request body"

echo "-- mixed-case multipart PUT is not claimed"
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X PUT \
    -H 'Content-Type: Multipart/Form-Data; boundary=unused' --data-binary '' "${BASE_URL}/${DB}/${TABLE}.CSV"

echo "-- a PUT whose Content-Type only starts with multipart/form-data is not claimed"
printf '1\n' \
    | curl --http1.1 -sS -o /dev/null -w 'HTTP %{http_code}\n' -X PUT --expect100-timeout 0 --max-time 60 \
        -H 'Content-Type: multipart/form-datax' -H 'Expect: 100-continue' -H 'X-ClickHouse-100-Continue: defer' \
        --data-binary @- "${BASE_URL}/${DB}/${TABLE}.CSV"

echo "-- a PUT path without a format suffix is not claimed"
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X PUT --data-binary 'SELECT 1' "${BASE_URL}/${DB}/${TABLE}"

echo "-- a missing table is rejected"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/missing_table_05029.CSV" 2>&1 \
    | grep -oE "UNKNOWN_TABLE" | head -n1

echo "-- an unknown path format is rejected"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.UnknownFormat" 2>&1 \
    | grep -oE "HTTP PUT table uploads require a known format"

echo "-- compression without a format is rejected"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.gz" 2>&1 \
    | grep -oE "Compression extension .* specified without a format"

echo "-- an empty body is rejected"
curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary '' "${BASE_URL}/${DB}/${TABLE}.CSV" 2>&1 \
    | grep -oE "HTTP PUT table uploads require a non-empty request body"

echo "-- an unframed PUT upload is rejected with 411"
python3 - <<PY
import socket


sock = socket.create_connection(("${CLICKHOUSE_HOST}", ${CLICKHOUSE_PORT_HTTP}), timeout=30)
try:
    body = b'44,"unframed"\n'
    sock.sendall(
        b"PUT /${DB}/${TABLE}.CSV HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: close\r\n\r\n"
        + body
    )
    response = sock.recv(4096)
finally:
    sock.close()

status = response.split(b" ", 2)[1].decode()
if status != "411":
    raise RuntimeError(f"expected HTTP 411, got HTTP {status}")
print("unframed-status:", status)
PY
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE} WHERE a = 44" | grep -qx '0'

echo "-- PUT with an explicit INSERT query is rejected"
printf '46,"query-insert"\n' \
    | curl -sS -w '\nHTTP %{http_code}\n' -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?query=INSERT%20INTO%20${TABLE}%20FORMAT%20CSV" 2>&1 \
    | grep -oE "HTTP PUT table uploads do not support the 'query' parameter|HTTP 400"

echo "-- PUT with an explicit SELECT query is rejected"
curl -sS -w '\nHTTP %{http_code}\n' -X PUT -H 'Content-Type: text/csv' --data-binary '' \
    "${BASE_URL}/${DB}/${TABLE}.CSV?query=SELECT+1" 2>&1 \
    | grep -oE "HTTP PUT table uploads do not support the 'query' parameter|HTTP 400"

echo "-- PUT with an empty query parameter is rejected"
curl -sS -w '\nHTTP %{http_code}\n' -X PUT -H 'Content-Type: text/csv' --data-binary '' \
    "${BASE_URL}/${DB}/${TABLE}.CSV?query=" 2>&1 \
    | grep -oE "HTTP PUT table uploads do not support the 'query' parameter|HTTP 400"

echo "-- query-bearing PUT inserted no rows"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE} WHERE a = 46"

echo "-- an explicit readonly setting remains enforced"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?readonly=1" 2>&1 \
    | grep -oE "Cannot execute query in readonly mode"

echo "-- PUT still requires the INSERT privilege"
printf '45,"no-insert"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?user=${NO_INSERT_USER}" 2>&1 \
    | grep -oE "Not enough privileges"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE} WHERE a = 45" | grep -qx '0'

echo "-- PUT to a missing table without the INSERT privilege fails exactly like a plain INSERT"
# The upload must not run the pre-auth table existence check with nearest-table hints: the only thing it may
# reveal is what the same user already learns from `INSERT INTO <table> FORMAT CSV` over the native protocol.
# Missing-table diagnostics must match the native INSERT path.
put_error=$(printf '45,"no-insert"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}_missing.CSV?user=${NO_INSERT_USER}" 2>&1 \
    | grep -oE "Code: [0-9]+|Maybe you meant[^.]*|maybe you meant[^.]*" | sort -u || true)
insert_error=$(printf '45,"no-insert"\n' \
    | ${CLICKHOUSE_CLIENT} --user "${NO_INSERT_USER}" -q "INSERT INTO ${DB}.${TABLE}_missing FORMAT CSV" 2>&1 \
    | grep -oE "Code: [0-9]+|Maybe you meant[^.]*|maybe you meant[^.]*" | sort -u || true)
[ "${put_error}" = "${insert_error}" ] && echo "same error: ${put_error}" || echo "PUT: ${put_error}; INSERT: ${insert_error}"

echo "-- failed requests drain bodies and keep connections reusable"
python3 - <<PY
import gzip
import socket


def read_response(sock):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("connection closed while reading response headers")
        data += chunk

    headers, body = data.split(b"\r\n\r\n", 1)
    header_lines = headers.split(b"\r\n")
    header_map = {}
    for line in header_lines[1:]:
        name, value = line.split(b":", 1)
        header_map[name.lower()] = value.strip().lower()

    if header_map.get(b"transfer-encoding") == b"chunked":
        while not (body.startswith(b"0\r\n\r\n") or b"\r\n0\r\n\r\n" in body):
            chunk = sock.recv(4096)
            if not chunk:
                raise RuntimeError("connection closed while reading chunked response")
            body += chunk
    elif b"content-length" in header_map:
        length = int(header_map[b"content-length"])
        while len(body) < length:
            chunk = sock.recv(4096)
            if not chunk:
                raise RuntimeError("connection closed while reading response body")
            body += chunk

    return headers, body


def put_request(path, body):
    headers = (
        f"PUT {path} HTTP/1.1\r\n"
        "Host: localhost\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: keep-alive\r\n\r\n"
    )
    return headers.encode() + body


def check_reuse(label, first_request, display=True):
    with socket.create_connection(("${CLICKHOUSE_HOST}", ${CLICKHOUSE_PORT_HTTP}), timeout=30) as sock:
        sock.sendall(first_request)
        first_headers, _ = read_response(sock)
        first_status = int(first_headers.split(b" ", 2)[1])
        if not 400 <= first_status < 600:
            raise RuntimeError(f"expected request failure, got HTTP {first_status}")
        first_connection_close = b"connection: close" in first_headers.lower()
        if first_connection_close:
            raise RuntimeError("error response closed a reusable connection")

        sock.sendall(
            b"GET /?query=SELECT+1 HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Connection: close\r\n\r\n"
        )
        second_headers, _ = read_response(sock)
        second_status = int(second_headers.split(b" ", 2)[1])
        if second_status != 200:
            raise RuntimeError(f"expected a reusable connection, got HTTP {second_status}")

    if display:
        first_prefix = f"{label}-first" if label else "first"
        second_prefix = f"{label}-second" if label else "second"
        print(f"{first_prefix}-status: {first_status}")
        print(f"{first_prefix}-connection-close: {first_connection_close}")
        print(f"{second_prefix}-status: {second_status}")


check_reuse("", put_request("/${DB}/${TABLE}.UnknownFormat", b'4,"four"\n'))
check_reuse("parser", put_request("/${DB}/${TABLE}.CSV", b'11,"parser"\nnot-a-number,"bad"\n'))
check_reuse(
    "compressed-parser",
    put_request("/${DB}/${TABLE}.CSV.gz", gzip.compress(b'15,"compressed-parser"\nnot-a-number,"bad"\n')),
)

check_reuse(
    "get-error",
    b"GET /?query=SELECT+FROM HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n",
    display=False,
)
print("get-error-reuse: OK")
check_reuse(
    "post-error",
    b"POST /?query=SELECT+FROM HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
    display=False,
)
print("post-error-reuse: OK")

with socket.create_connection(("${CLICKHOUSE_HOST}", ${CLICKHOUSE_PORT_HTTP}), timeout=30) as sock:
    sock.sendall(
        b"PUT /${DB}/${TABLE}.CSV HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"Connection: close\r\n\r\n"
        b"0\r\n\r\n"
    )
    chunked_empty_headers, _ = read_response(sock)
chunked_empty_status = int(chunked_empty_headers.split(b" ", 2)[1])
if chunked_empty_status != 400:
    raise RuntimeError(f"expected empty-body rejection, got HTTP {chunked_empty_status}")
print("chunked-empty-status:", chunked_empty_status)
PY
