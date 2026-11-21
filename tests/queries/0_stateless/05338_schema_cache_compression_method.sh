#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

user_files_path="${CLICKHOUSE_USER_FILES:-/var/lib/clickhouse/user_files}"

gz_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_gz.raw"
rev_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_rev.raw"
plain_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_plain.raw"
same_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_same.raw"
match_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_match.csv.gz"
snappy_basic_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_snappy_basic.raw"
snappy_framed_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_snappy_framed.raw"

cleanup()
{
    if [ -n "${HTTP_PID:-}" ]; then
        kill "$HTTP_PID" 2>/dev/null || true
        wait "$HTTP_PID" 2>/dev/null || true
    fi
    rm -rf "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"* "${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}"*
}
trap cleanup EXIT

assert_exception()
{
    local expected_code="$1"
    shift
    local out=""
    local rc=0

    set +e
    out=$("$@" 2>&1)
    rc=$?
    set -e

    if [ "$rc" -eq 0 ]; then
        echo "Unexpected success: command was expected to fail with $expected_code: $*" >&2
        echo "Command output: $out" >&2
        exit 1
    fi

    local pattern="$expected_code"
    if [ "$expected_code" = "CANNOT_DECOMPRESS" ]; then
        pattern="CANNOT_DECOMPRESS|ZLIB_INFLATE_FAILED"
    elif [ "$expected_code" = "SNAPPY_UNCOMPRESS_FAILED" ]; then
        pattern="SNAPPY_UNCOMPRESS_FAILED|CANNOT_DECOMPRESS"
    fi

    if ! echo "$out" | grep -q -E "$pattern"; then
        echo "Unexpected error: expected $expected_code, but got rc=$rc: $*" >&2
        echo "Command output: $out" >&2
        exit 1
    fi

    echo "$expected_code"
}

# 1. Create test files
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$gz_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$rev_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$plain_file', 'CSV', 'a UInt8, b UInt8') SELECT 1, 2 SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$same_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(50) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$match_file', 'CSV', 'x UInt64', 'gzip') SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1"
has_snappy=0
if [ "$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.build_options WHERE name = 'USE_SNAPPY' LIMIT 1" 2>/dev/null || echo 0)" = "1" ]; then
    has_snappy=1
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$snappy_basic_file', 'LineAsString', 'line String', 'snappy') SELECT toString(number) FROM numbers(50) SETTINGS engine_file_truncate_on_insert = 1, snappy_mode = 'basic'"
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$snappy_framed_file', 'LineAsString', 'line String', 'snappy') SELECT toString(number) FROM numbers(50) SETTINGS engine_file_truncate_on_insert = 1, snappy_mode = 'framed'"
fi

# Bounded wait to ensure file modification timestamp is older than current second for cache registration
synced=0
for _ in {1..30}; do
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT now() > (SELECT max(_time) FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_*.raw', 'One'))")" = "1" ]; then
        synced=1
        break
    fi
    sleep 0.1
done

if [ "$synced" -ne 1 ]; then
    echo "Timed out waiting for file modification timestamp synchronization" >&2
    exit 1
fi

settings="--use_cache_for_count_from_files=1 --optimize_count_from_files=1 --schema_inference_use_cache_for_file=1"

# Case A: File wrong row count (auto then explicit gzip)
echo "Case A: File wrong row count"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$gz_file', 'LineAsString', 'line String')" > /dev/null
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$gz_file', 'LineAsString', 'line String', 'gzip')"

# Case B: File reverse order (explicit gzip then auto)
echo "Case B: File reverse order"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$rev_file', 'LineAsString', 'line String', 'gzip')"
c=$($CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$rev_file', 'LineAsString', 'line String')")
if [ "$c" -ne 1000 ]; then
    echo "NOT_1000"
else
    echo "STALE_CACHE_1000"
fi

# Case C: File schema inference (auto then explicit gzip on plain CSV)
echo "Case C: File schema inference"
$CLICKHOUSE_CLIENT $settings -q "DESCRIBE file('$plain_file', 'CSV')" > /dev/null
assert_exception "CANNOT_DECOMPRESS" $CLICKHOUSE_CLIENT $settings -q "DESCRIBE file('$plain_file', 'CSV', 'auto', 'gzip')"

# Case D: File same-mode reuse (repeated explicit gzip)
echo "Case D: File same-mode reuse"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$same_file', 'LineAsString', 'line String', 'gzip')"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$same_file', 'LineAsString', 'line String', 'gzip')"

# Case E: File auto versus explicit codec matching effective codec
echo "Case E: File auto vs explicit matching"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$match_file', 'CSV', 'x UInt64')"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$match_file', 'CSV', 'x UInt64', 'gzip')"

# Case F: Same-URL row count collision
echo "Case F: Same-URL row count collision"
url_settings="--use_cache_for_count_from_files=1 --optimize_count_from_files=1 --schema_inference_use_cache_for_url=1 --schema_inference_cache_require_modification_time_for_url=0"
url_same_count="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_count"

$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_same_count', 'CSV', 'x UInt64', 'none')"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_same_count', 'CSV', 'x UInt64', 'none')"
assert_exception "CANNOT_DECOMPRESS" $CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_same_count', 'CSV', 'x UInt64', 'gzip')"

# Case G: Same-URL schema inference collision
echo "Case G: Same-URL schema inference collision"
url_same_schema="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_schema"

$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_same_schema', 'CSV', 'auto', 'none') FORMAT TSV" | cut -f1,2
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_same_schema', 'CSV', 'auto', 'none') FORMAT TSV" | cut -f1,2
assert_exception "CANNOT_DECOMPRESS" $CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_same_schema', 'CSV', 'auto', 'gzip')"

# Case H: Positive URL gzip caching
echo "Case H: Positive URL gzip caching"
url_gzip="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&enable_http_compression=1&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_gz"

$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_gzip', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'gzip'))"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_gzip', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'gzip'))"
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_gzip', 'CSV', 'auto', 'gzip', headers('Accept-Encoding' = 'gzip')) FORMAT TSV" | cut -f1,2

# Case I: Schema cache system table inspection
echo "Case I: Schema cache system table inspection"
$CLICKHOUSE_CLIENT -q "
SELECT storage, format, compression_method, number_of_rows
FROM system.schema_inference_cache
WHERE storage = 'URL' AND source LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}_count%'
SETTINGS schema_inference_use_cache_for_url=0
"
$CLICKHOUSE_CLIENT -q "
SELECT storage, format, compression_method, number_of_rows
FROM system.schema_inference_cache
WHERE storage = 'URL' AND source LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}_gz%'
SETTINGS schema_inference_use_cache_for_url=0
"
$CLICKHOUSE_CLIENT -q "
SELECT storage, format, compression_method, number_of_rows
FROM system.schema_inference_cache
WHERE storage = 'File' AND format = 'LineAsString' AND source LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}_same.raw%'
SETTINGS schema_inference_use_cache_for_file=0
"

# Case J: Header sensitivity: plain request first, gzip request second
echo "Case J: Header sensitivity plain first then gzip"
url_hdr_plain="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&enable_http_compression=1&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_hdr_plain"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_hdr_plain', 'CSV', 'x UInt64', 'none')"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_hdr_plain', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'gzip'))"

# Case K: Header sensitivity: gzip request first, plain request second with compression mismatch
echo "Case K: Header sensitivity gzip first then plain"
url_hdr_rev="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&enable_http_compression=1&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_hdr_rev"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_hdr_rev', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'gzip'))"
assert_exception "CANNOT_DECOMPRESS" $CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_hdr_rev', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'identity'))"

# Case L: Header case and whitespace normalization reuse
echo "Case L: Header canonicalization reuse"
url_hdr_canon="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&enable_http_compression=1&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_hdr_canon"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_hdr_canon', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'gzip'))"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_hdr_canon', 'CSV', 'x UInt64', 'gzip', headers('accept-encoding' = ' gzip '))"

# Case M: Secret header redaction in schema inference cache
echo "Case M: Secret header redaction"
url_secret="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_secret"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_secret', 'CSV', 'x UInt64', 'none', headers('X-Private-Data' = 'secret-key-value-12345'))"
$CLICKHOUSE_CLIENT -q "
SELECT storage, format, headers LIKE '%secret-key-value%', headers LIKE '%x-private-data%=%[HIDDEN]%'
FROM system.schema_inference_cache
WHERE storage = 'URL' AND source LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}_secret%'
SETTINGS schema_inference_use_cache_for_url=0
"

# Case N: Arbitrary custom credential header redaction in schema inference cache
echo "Case N: Arbitrary custom credential header redaction"
url_custom_secret="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_custom_secret"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_custom_secret', 'CSV', 'x UInt64', 'none', headers('X-Opaque' = 'opaque-secret-value-777', 'X-Internal-Metadata' = 'confidential-customer-blob'))"
$CLICKHOUSE_CLIENT -q "
SELECT storage, format, headers LIKE '%opaque-secret%', headers LIKE '%confidential-customer%', headers LIKE '%x-opaque%=%[HIDDEN]%', headers LIKE '%x-internal-metadata%=%[HIDDEN]%'
FROM system.schema_inference_cache
WHERE storage = 'URL' AND source LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}_custom_secret%'
SETTINGS schema_inference_use_cache_for_url=0
"

# Case O: Duplicate header order sensitivity in count()
echo "Case O: Duplicate header order sensitivity count"
url_dup="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&enable_http_compression=1&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_dup"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_dup', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'identity', 'Accept-Encoding' = 'gzip'))"
assert_exception "CANNOT_DECOMPRESS" $CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_dup', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'gzip', 'Accept-Encoding' = 'identity'))"

# Case P: Duplicate header order sensitivity in schema inference
echo "Case P: Duplicate header order sensitivity schema inference"
url_dup_schema="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&enable_http_compression=1&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_dup_schema"
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_dup_schema', 'CSV', 'auto', 'gzip', headers('Accept-Encoding' = 'identity', 'Accept-Encoding' = 'gzip')) FORMAT TSV" | cut -f1,2
assert_exception "CANNOT_DECOMPRESS" $CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_dup_schema', 'CSV', 'auto', 'gzip', headers('Accept-Encoding' = 'gzip', 'Accept-Encoding' = 'identity'))"

# Case Q: Snappy wire mode file count cache isolation
echo "Case Q: Snappy wire mode file count cache isolation"
if [ "$has_snappy" -eq 1 ]; then
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$snappy_basic_file', 'LineAsString', 'line String', 'snappy') SETTINGS use_cache_for_count_from_files=1, snappy_mode='basic'"
    assert_exception "SNAPPY_UNCOMPRESS_FAILED" $CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$snappy_basic_file', 'LineAsString', 'line String', 'snappy') SETTINGS use_cache_for_count_from_files=1, snappy_mode='framed'"
else
    echo 50
    echo "SNAPPY_UNCOMPRESS_FAILED"
fi

# Case R: Snappy wire mode reverse order file count cache isolation
echo "Case R: Snappy wire mode reverse order file count cache isolation"
if [ "$has_snappy" -eq 1 ]; then
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$snappy_framed_file', 'LineAsString', 'line String', 'snappy') SETTINGS use_cache_for_count_from_files=1, snappy_mode='framed'"
    assert_exception "SNAPPY_UNCOMPRESS_FAILED" $CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$snappy_framed_file', 'LineAsString', 'line String', 'snappy') SETTINGS use_cache_for_count_from_files=1, snappy_mode='basic'"
else
    echo 50
    echo "SNAPPY_UNCOMPRESS_FAILED"
fi

# Case S: Explicit tracing header row count cache partitioning
echo "Case S: Explicit tracing header row count cache partitioning"
url_trace_count="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(2)+WHERE+number+%3D+0+OR+getClientHTTPHeader(%27tracestate%27)+%3D+%27vendor%3Dtwo%27+FORMAT+CSV&allow_get_client_http_header=1&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_trace_count"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_trace_count', 'CSV', 'x UInt64', 'none', headers('tracestate' = 'vendor=one'))"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_trace_count', 'CSV', 'x UInt64', 'none', headers('tracestate' = 'vendor=two'))"

# Case T: Explicit tracing header schema inference partitioning
echo "Case T: Explicit tracing header schema inference partitioning"
url_trace_schema="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1+AS+col1%2C+if(getClientHTTPHeader(%27tracestate%27)+%3D+%27vendor%3Dtwo%27%2C+%27string_val%27%2C+%27100%27)+AS+col2+FORMAT+TSVWithNames&allow_get_client_http_header=1&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_trace_schema"
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_trace_schema', 'TSVWithNames', 'auto', 'none', headers('tracestate' = 'vendor=one')) FORMAT TSV" | cut -f1,2
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_trace_schema', 'TSVWithNames', 'auto', 'none', headers('tracestate' = 'vendor=two')) FORMAT TSV" | cut -f1,2

# Case U: Auto-tracing does not fragment schema cache
echo "Case U: Auto-tracing does not fragment schema cache"
url_autotrace="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_autotrace"
$CLICKHOUSE_CLIENT $url_settings --opentelemetry_start_trace_probability=1 -q "SELECT count() FROM url('$url_autotrace', 'CSV', 'x UInt64', 'none')"
$CLICKHOUSE_CLIENT $url_settings --opentelemetry_start_trace_probability=1 -q "SELECT count() FROM url('$url_autotrace', 'CSV', 'x UInt64', 'none')"
$CLICKHOUSE_CLIENT -q "
SELECT storage, format, count()
FROM system.schema_inference_cache
WHERE storage = 'URL' AND source LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}_autotrace%'
GROUP BY storage, format
SETTINGS schema_inference_use_cache_for_url=0
"

# Case V: Wildcard URL header cache isolation
echo "Case V: Wildcard URL header cache isolation"

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
try:
    from http.server import ThreadingHTTPServer as ServerClass, BaseHTTPRequestHandler
except ImportError:
    from http.server import HTTPServer as ServerClass, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_HEAD(self):
        self.handle_req(False)

    def do_GET(self):
        self.handle_req(True)

    def handle_req(self, with_body):
        path = self.path.split('?')[0]
        if path == '/data/':
            data = b'<a href=\"a.csv\">a.csv</a>\n'
            ctype = 'text/html'
        elif path == '/data/a.csv':
            variant = self.headers.get('X-Variant', '')
            if variant == 'one':
                data = b'val_one\n1\n'
            elif variant == 'two':
                data = b'val_two\n10\n20\n'
            else:
                data = b'val_default\n999\n'
            ctype = 'text/csv'
        else:
            self.send_response(404)
            self.end_headers()
            return

        range_hdr = self.headers.get('Range')
        if range_hdr and range_hdr.startswith('bytes='):
            parts = range_hdr[6:].split('-')
            start = int(parts[0]) if parts[0] else 0
            end = int(parts[1]) if len(parts) > 1 and parts[1] else len(data) - 1
            if start < len(data):
                end = min(end, len(data) - 1)
                sliced = data[start:end+1]
                self.send_response(206)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Range', f'bytes {start}-{end}/{len(data)}')
                self.send_header('Content-Length', str(len(sliced)))
                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('Last-Modified', 'Sun, 01 Jan 2023 00:00:00 GMT')
                self.end_headers()
                if with_body:
                    self.wfile.write(sliced)
                return
            else:
                self.send_response(416)
                self.send_header('Content-Range', f'bytes */{len(data)}')
                self.end_headers()
                return

        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Last-Modified', 'Sun, 01 Jan 2023 00:00:00 GMT')
        self.end_headers()
        if with_body:
            self.wfile.write(data)

ServerClass(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/data/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

wildcard_settings="--allow_url_wildcard_from_index_pages=1 --use_cache_for_count_from_files=1 --optimize_count_from_files=1 --schema_inference_use_cache_for_url=1 --schema_inference_cache_require_modification_time_for_url=0 --send_logs_level=error"
wildcard_url="http://127.0.0.1:${HTTP_PORT}/data/*.csv"

# First count with X-Variant: one warms cache with 1 row
$CLICKHOUSE_CLIENT $wildcard_settings -q "SELECT count() FROM url('$wildcard_url', 'CSV', 'val_one UInt64', headers('X-Variant' = 'one'))"

# Second count with X-Variant: two must not hit the 'one' entry and must return 2 rows
$CLICKHOUSE_CLIENT $wildcard_settings -q "SELECT count() FROM url('$wildcard_url', 'CSV', 'val_two UInt64', headers('X-Variant' = 'two'))"

# Re-read with X-Variant: two hits cache
$CLICKHOUSE_CLIENT $wildcard_settings -q "SELECT count() FROM url('$wildcard_url', 'CSV', 'val_two UInt64', headers('X-Variant' = 'two'))"

# Header case and whitespace canonicalization
$CLICKHOUSE_CLIENT $wildcard_settings -q "SELECT count() FROM url('$wildcard_url', 'CSV', 'val_two UInt64', headers('x-variant' = 'two'))"

# Re-read with X-Variant: one hits its separate cache entry
$CLICKHOUSE_CLIENT $wildcard_settings -q "SELECT count() FROM url('$wildcard_url', 'CSV', 'val_one UInt64', headers('X-Variant' = 'one'))"

# Schema inference partitioning
$CLICKHOUSE_CLIENT $wildcard_settings -q "DESCRIBE url('$wildcard_url', 'CSV', 'auto', headers('X-Variant' = 'one')) FORMAT TSV" | cut -f1,2
$CLICKHOUSE_CLIENT $wildcard_settings -q "DESCRIBE url('$wildcard_url', 'CSV', 'auto', headers('X-Variant' = 'two')) FORMAT TSV" | cut -f1,2

# Table engine with wildcard URL and headers
$CLICKHOUSE_CLIENT $wildcard_settings -q "
DROP TABLE IF EXISTS table_wildcard_v;
CREATE TABLE table_wildcard_v (val_one UInt64) ENGINE = URL('$wildcard_url', 'CSV', 'none', headers('X-Variant' = 'one'));
SELECT count() FROM table_wildcard_v;
DROP TABLE table_wildcard_v;
"

# Case W: Archive member snappy wire mode cache isolation
echo "Case W: Archive member snappy wire mode cache isolation"
has_libarchive=1
if ! command -v tar >/dev/null 2>&1; then
    has_libarchive=0
else
    mkdir -p "${user_files_path}" "${CLICKHOUSE_TMP}" 2>/dev/null || true
    probe_tar="${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_probe.tar"
    probe_csv="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_probe.csv"
    echo "1" > "${probe_csv}"
    if tar -cf "${probe_tar}" -C "${CLICKHOUSE_TMP}" "${CLICKHOUSE_TEST_UNIQUE_NAME}_probe.csv" 2>/dev/null; then
        archive_probe_output=$($CLICKHOUSE_CLIENT --allow_archive_path_syntax=1 -q "DESCRIBE file('${CLICKHOUSE_TEST_UNIQUE_NAME}_probe.tar::${CLICKHOUSE_TEST_UNIQUE_NAME}_probe.csv', 'CSV')" 2>&1 || true)
        rm -f "${probe_tar}" "${probe_csv}" 2>/dev/null || true
        if echo "$archive_probe_output" | grep -qE "libarchive library is disabled|SUPPORT_IS_DISABLED"; then
            has_libarchive=0
        fi
    else
        rm -f "${probe_tar}" "${probe_csv}" 2>/dev/null || true
        has_libarchive=0
    fi
fi

if [ "$has_snappy" -eq 1 ] && [ "$has_libarchive" -eq 1 ]; then
    archive_members_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive_members"
    mkdir -p "${archive_members_dir}"

    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_part_basic.snappy', 'CSV', 'x UInt64', 'snappy') SELECT number FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1, snappy_mode = 'basic'"
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_part_framed.snappy', 'CSV', 'x UInt64', 'snappy') SELECT number FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1, snappy_mode = 'framed'"

    cp "${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_part_basic.snappy" "${archive_members_dir}/part.snappy"
    tar -cf "${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_basic.tar" -C "${archive_members_dir}" part.snappy

    cp "${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_part_framed.snappy" "${archive_members_dir}/part.snappy"
    tar -cf "${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_framed.tar" -C "${archive_members_dir}" part.snappy

    touch -d '2000-01-01' "${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_basic.tar" "${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_framed.tar"

    archive_basic="${CLICKHOUSE_TEST_UNIQUE_NAME}_basic.tar"
    archive_framed="${CLICKHOUSE_TEST_UNIQUE_NAME}_framed.tar"

    $CLICKHOUSE_CLIENT -q "DESCRIBE file('$archive_basic::part.snappy', 'CSV', 'auto', 'snappy') SETTINGS snappy_mode='basic', schema_inference_use_cache_for_file=1 FORMAT TSV" | cut -f1,2
    assert_exception "SNAPPY_UNCOMPRESS_FAILED" $CLICKHOUSE_CLIENT -q "DESCRIBE file('$archive_basic::part.snappy', 'CSV', 'auto', 'snappy') SETTINGS snappy_mode='framed', schema_inference_use_cache_for_file=1"

    $CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$archive_basic::part.snappy', 'CSV', 'x UInt64', 'snappy') SETTINGS snappy_mode='basic', use_cache_for_count_from_files=1"
    assert_exception "SNAPPY_UNCOMPRESS_FAILED" $CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$archive_basic::part.snappy', 'CSV', 'x UInt64', 'snappy') SETTINGS snappy_mode='framed', use_cache_for_count_from_files=1"

    $CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$archive_framed::part.snappy', 'CSV', 'x UInt64', 'snappy') SETTINGS snappy_mode='framed', use_cache_for_count_from_files=1"
    assert_exception "SNAPPY_UNCOMPRESS_FAILED" $CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$archive_framed::part.snappy', 'CSV', 'x UInt64', 'snappy') SETTINGS snappy_mode='basic', use_cache_for_count_from_files=1"
else
    echo "c1	Nullable(Int64)"
    echo "SNAPPY_UNCOMPRESS_FAILED"
    echo 10
    echo "SNAPPY_UNCOMPRESS_FAILED"
    echo 10
    echo "SNAPPY_UNCOMPRESS_FAILED"
fi

# Case X: Ordinary filename with #compression_method= marker
echo "Case X: Ordinary filename with #compression_method= marker"
special_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_rows#compression_method=tag.raw"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$special_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1"

touch -d '2000-01-01' "${user_files_path}/${special_file}"

count_none=$($CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$special_file', 'LineAsString', 'line String', 'none')")
if [ "$count_none" -ne 1000 ]; then
    echo "NOT_1000"
else
    echo "UNEXPECTED_1000"
fi

$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$special_file', 'LineAsString', 'line String', 'gzip')"
