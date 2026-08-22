#!/usr/bin/env bash

set -euo pipefail

# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
TABLE="put_table_05029"
DELETE_HANDLER="put_delete_exception_05029"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP HANDLER IF EXISTS \`${DELETE_HANDLER}\`"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.${TABLE}"
}
trap cleanup EXIT

cleanup
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.${TABLE} (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER \`${DELETE_HANDLER}\` URL '/${DELETE_HANDLER}' METHODS (DELETE) AS SELECT throwIf(number = 0, '05029 delete failure') FROM numbers(1) FORMAT TSV"

echo "===== PUT table upload ====="
echo "-- CSV format from the path"
printf '1,"one"\n2,"two"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.CSV"

echo "-- JSONEachRow format from the path"
printf '{"a":3,"b":"three"}\n' \
    | curl -sS -X PUT -H 'Content-Type: application/json' --data-binary @- "${BASE_URL}/${DB}/${TABLE}.JSONEachRow"

echo "-- inserted rows"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB}.${TABLE} ORDER BY a"

echo "-- database-prefixed upload works with table-as-file disabled"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?http_allow_table_as_file=0"

echo "-- unqualified upload still requires table-as-file"
printf '5,"five"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${TABLE}.CSV?http_allow_database_as_path=1&http_allow_table_as_file=0" 2>&1 \
    | grep -oE "HTTP PUT table uploads require a table path with a known format"

echo "-- unqualified upload works with table-as-file enabled"
printf '5,"five"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${TABLE}.csv?http_allow_database_as_path=0&http_allow_table_as_file=1"

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
    | grep -oE "Conflicting compression: .*Content-Encoding"

echo "-- inserted rows after compressed uploads"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB}.${TABLE} ORDER BY a"

echo "-- a PUT path without a format suffix is not claimed"
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X PUT --data-binary 'SELECT 1' "${BASE_URL}/${DB}/${TABLE}"

echo "-- a missing table is rejected"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/missing_table_05029.CSV" 2>&1 \
    | grep -oE "which does not exist"

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
    body = b'14,"unframed"\n'
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

echo "-- an explicit INSERT query remains read-only on PUT"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?query=INSERT%20INTO%20${TABLE}%20FORMAT%20CSV" 2>&1 \
    | grep -oE "Cannot execute query in readonly mode"

echo "-- an explicit readonly setting remains enforced"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?readonly=1" 2>&1 \
    | grep -oE "Cannot execute query in readonly mode"

echo "-- a failed PUT drains its body so the connection can be reused"
python3 - <<PY
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


sock = socket.create_connection(("${CLICKHOUSE_HOST}", ${CLICKHOUSE_PORT_HTTP}), timeout=30)
try:
    body = b'4,"four"\n'
    sock.sendall(
        b"PUT /${DB}/${TABLE}.UnknownFormat HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: keep-alive\r\n\r\n"
        + body
    )
    first_headers, _ = read_response(sock)

    sock.sendall(
        b"GET /?query=SELECT+1 HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: close\r\n\r\n"
    )
    second_headers, _ = read_response(sock)
finally:
    sock.close()

print("first-status:", first_headers.split(b" ", 2)[1].decode())
print("first-connection-close:", b"connection: close" in first_headers.lower())
print("second-status:", second_headers.split(b" ", 2)[1].decode())

delete_sock = socket.create_connection(("${CLICKHOUSE_HOST}", ${CLICKHOUSE_PORT_HTTP}), timeout=30)
try:
    body = b"unused delete body"
    delete_sock.sendall(
        b"DELETE /${DELETE_HANDLER} HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: keep-alive\r\n\r\n"
        + body
    )
    delete_first_headers, _ = read_response(delete_sock)

    delete_sock.sendall(
        b"GET /?query=SELECT+1 HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: close\r\n\r\n"
    )
    delete_second_headers, _ = read_response(delete_sock)
finally:
    delete_sock.close()

print("delete-first-status:", delete_first_headers.split(b" ", 2)[1].decode())
print("delete-first-connection-close:", b"connection: close" in delete_first_headers.lower())
print("delete-second-status:", delete_second_headers.split(b" ", 2)[1].decode())
PY
