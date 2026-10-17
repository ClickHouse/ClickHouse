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
