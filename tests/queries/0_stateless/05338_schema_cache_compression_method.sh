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
    rm -f "${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}"*
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
url_trace_count="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(2)+WHERE+number+%3D+0+OR+getClientHTTPHeader('tracestate')+%3D+'vendor%3Dtwo'+FORMAT+CSV&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_trace_count"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_trace_count', 'CSV', 'x UInt64', 'none', headers('tracestate' = 'vendor=one'))"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_trace_count', 'CSV', 'x UInt64', 'none', headers('tracestate' = 'vendor=two'))"

# Case T: Explicit tracing header schema inference partitioning
echo "Case T: Explicit tracing header schema inference partitioning"
url_trace_schema="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1+AS+col1%2C+if(getClientHTTPHeader('tracestate')+%3D+'vendor%3Dtwo'%2C+'string_val'%2C+'100')+AS+col2+FORMAT+CSVWithNames&query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_trace_schema"
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_trace_schema', 'CSVWithNames', 'auto', 'none', headers('tracestate' = 'vendor=one')) FORMAT TSV" | cut -f1,2
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_trace_schema', 'CSVWithNames', 'auto', 'none', headers('tracestate' = 'vendor=two')) FORMAT TSV" | cut -f1,2

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
