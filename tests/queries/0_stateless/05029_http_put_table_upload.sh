#!/usr/bin/env bash

set -euo pipefail

# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
TABLE="put_table_05029"
QUOTED_TABLE="a=1_05029"
DELETE_HANDLER="put_delete_exception_05029_${DB}"
NO_INSERT_USER="put_no_insert_05029_${DB}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP HANDLER IF EXISTS \`${DELETE_HANDLER}\`"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS \`${NO_INSERT_USER}\`"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.${TABLE}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.\`${QUOTED_TABLE}\`"
}
trap cleanup EXIT

cleanup
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.${TABLE} (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.\`${QUOTED_TABLE}\` (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "CREATE USER \`${NO_INSERT_USER}\` IDENTIFIED WITH no_password SETTINGS http_allow_database_as_path = 1, http_allow_table_as_file = 1"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${DB}.${TABLE} TO \`${NO_INSERT_USER}\`"
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

echo "-- quoted table names with filter characters support upload suffixes"
printf '42,"quoted"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/%60a%3D1_05029%60.CSV?http_allow_filters_as_path=1" >/dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.\`${QUOTED_TABLE}\` WHERE a = 42" | grep -qx '1'

echo "-- quoted table names support compressed upload suffixes"
printf '43,"quoted-gzip"\n' \
    | gzip -c \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/%60a%3D1_05029%60.CSV.gz?http_allow_filters_as_path=1" >/dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.\`${QUOTED_TABLE}\` WHERE a = 43" | grep -qx '1'

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

echo "-- a PUT path without a format suffix is not claimed"
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X PUT --data-binary 'SELECT 1' "${BASE_URL}/${DB}/${TABLE}"

echo "-- a dot inside a quoted table name is not a format suffix"
curl --path-as-is -sS -o /dev/null -w 'HTTP %{http_code}\n' -X PUT --data-binary 'SELECT 1' \
    "${BASE_URL}/${DB}/%60events%2EJSON%60"

echo "-- a missing table is rejected"
printf '4,"four"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- "${BASE_URL}/${DB}/missing_table_05029.CSV" 2>&1 \
    | grep -oE "which does not exist"

echo "-- an unknown format after a quoted table is rejected"
printf '46,"quoted-unknown"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/%60a%3D1_05029%60.UnknownFormat?http_allow_filters_as_path=1" 2>&1 \
    | grep -oE "Unknown format 'UnknownFormat' in URL path"

echo "-- an unknown format after a quoted table is rejected for reads"
curl -sS "${BASE_URL}/${DB}/%60a%3D1_05029%60.UnknownFormat?http_allow_filters_as_path=1" 2>&1 \
    | grep -oE "Unknown format 'UnknownFormat' in URL path"

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

echo "-- PUT still requires the INSERT privilege"
printf '45,"no-insert"\n' \
    | curl -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV?user=${NO_INSERT_USER}" 2>&1 \
    | grep -oE "Not enough privileges"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE} WHERE a = 45" | grep -qx '0'

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

parser_sock = socket.create_connection(("${CLICKHOUSE_HOST}", ${CLICKHOUSE_PORT_HTTP}), timeout=30)
try:
    body = b'11,"parser"\nnot-a-number,"bad"\n'
    parser_sock.sendall(
        b"PUT /${DB}/${TABLE}.CSV HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: keep-alive\r\n\r\n"
        + body
    )
    parser_first_headers, _ = read_response(parser_sock)

    parser_sock.sendall(
        b"GET /?query=SELECT+1 HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: close\r\n\r\n"
    )
    parser_second_headers, _ = read_response(parser_sock)
finally:
    parser_sock.close()

parser_first_status = parser_first_headers.split(b" ", 2)[1]
if not 400 <= int(parser_first_status) < 600:
    raise RuntimeError(f"expected parser failure, got HTTP {parser_first_status.decode()}")
print("parser-first-status:", parser_first_status.decode())
print("parser-first-connection-close:", b"connection: close" in parser_first_headers.lower())
print("parser-second-status:", parser_second_headers.split(b" ", 2)[1].decode())

compressed_parser_sock = socket.create_connection(("${CLICKHOUSE_HOST}", ${CLICKHOUSE_PORT_HTTP}), timeout=30)
try:
    body = gzip.compress(b'15,"compressed-parser"\nnot-a-number,"bad"\n')
    compressed_parser_sock.sendall(
        b"PUT /${DB}/${TABLE}.CSV.gz HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: keep-alive\r\n\r\n"
        + body
    )
    compressed_parser_first_headers, _ = read_response(compressed_parser_sock)

    compressed_parser_sock.sendall(
        b"GET /?query=SELECT+1 HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: close\r\n\r\n"
    )
    compressed_parser_second_headers, _ = read_response(compressed_parser_sock)
finally:
    compressed_parser_sock.close()

compressed_parser_first_status = compressed_parser_first_headers.split(b" ", 2)[1]
if not 400 <= int(compressed_parser_first_status) < 600:
    raise RuntimeError(f"expected compressed parser failure, got HTTP {compressed_parser_first_status.decode()}")
print("compressed-parser-first-status:", compressed_parser_first_status.decode())
print("compressed-parser-first-connection-close:", b"connection: close" in compressed_parser_first_headers.lower())
print("compressed-parser-second-status:", compressed_parser_second_headers.split(b" ", 2)[1].decode())

chunked_sock = socket.create_connection(("${CLICKHOUSE_HOST}", ${CLICKHOUSE_PORT_HTTP}), timeout=30)
try:
    chunked_sock.sendall(
        b"PUT /${DB}/${TABLE}.CSV HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"Connection: close\r\n\r\n"
        b"0\r\n\r\n"
    )
    chunked_empty_headers, _ = read_response(chunked_sock)
finally:
    chunked_sock.close()

print("chunked-empty-status:", chunked_empty_headers.split(b" ", 2)[1].decode())
PY
