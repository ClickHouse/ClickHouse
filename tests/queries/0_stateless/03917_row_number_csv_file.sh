#!/usr/bin/env bash
# Tags: no-fasttest
# Test _row_number virtual column for CSV format with and without parallel parsing
# for File, URL, and S3 engines.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_NAME="test_csv_row_number_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# File engine
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_NAME}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_NAME} (a UInt32, b String) ENGINE = File(CSV)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE_NAME} VALUES (10, 'x'), (20, 'y'), (30, 'z'), (40, 'w'), (50, 'v')"

echo "--- File: a + _row_number, parallel_parsing=0"
${CLICKHOUSE_CLIENT} --query "SELECT a, _row_number FROM ${TABLE_NAME} ORDER BY a SETTINGS input_format_parallel_parsing = 0"
echo "--- File: a + _row_number, parallel_parsing=1"
${CLICKHOUSE_CLIENT} --query "SELECT a, _row_number FROM ${TABLE_NAME} ORDER BY a SETTINGS input_format_parallel_parsing = 1"

echo "--- File: _row_number only, parallel_parsing=0"
${CLICKHOUSE_CLIENT} --query "SELECT _row_number FROM ${TABLE_NAME} ORDER BY _row_number SETTINGS input_format_parallel_parsing = 0"
echo "--- File: _row_number only, parallel_parsing=1"
${CLICKHOUSE_CLIENT} --query "SELECT _row_number FROM ${TABLE_NAME} ORDER BY _row_number SETTINGS input_format_parallel_parsing = 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE_NAME}"

# S3 engine (Minio via named collection)
S3_FILENAME="${CLICKHOUSE_DATABASE}/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename='${S3_FILENAME}', format='CSV', structure='a UInt32, b String') VALUES (10, 'x'), (20, 'y'), (30, 'z'), (40, 'w'), (50, 'v')"

echo "--- S3: a + _row_number, parallel_parsing=0"
${CLICKHOUSE_CLIENT} --query "SELECT a, _row_number FROM s3(s3_conn, filename='${S3_FILENAME}', format='CSV', structure='a UInt32, b String') ORDER BY a SETTINGS input_format_parallel_parsing = 0"
echo "--- S3: a + _row_number, parallel_parsing=1"
${CLICKHOUSE_CLIENT} --query "SELECT a, _row_number FROM s3(s3_conn, filename='${S3_FILENAME}', format='CSV', structure='a UInt32, b String') ORDER BY a SETTINGS input_format_parallel_parsing = 1"

echo "--- S3: _row_number only, parallel_parsing=0"
${CLICKHOUSE_CLIENT} --query "SELECT _row_number FROM s3(s3_conn, filename='${S3_FILENAME}', format='CSV', structure='a UInt32, b String') ORDER BY _row_number SETTINGS input_format_parallel_parsing = 0"
echo "--- S3: _row_number only, parallel_parsing=1"
${CLICKHOUSE_CLIENT} --query "SELECT _row_number FROM s3(s3_conn, filename='${S3_FILENAME}', format='CSV', structure='a UInt32, b String') ORDER BY _row_number SETTINGS input_format_parallel_parsing = 1"

# URL engine (via ClickHouse HTTP interface serving CSV with deterministic row order)
URL_CSV="http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=$(${CLICKHOUSE_CLIENT} --query "SELECT encodeURLComponent('SELECT * FROM VALUES(''a UInt32, b String'', (10, ''x''), (20, ''y''), (30, ''z''), (40, ''w''), (50, ''v'')) ORDER BY a FORMAT CSV')")"

echo "--- URL: a + _row_number, parallel_parsing=0"
${CLICKHOUSE_CLIENT} --query "SELECT c1, _row_number FROM url('${URL_CSV}', CSV, 'c1 UInt32, c2 String') ORDER BY c1 SETTINGS input_format_parallel_parsing = 0"
echo "--- URL: a + _row_number, parallel_parsing=1"
${CLICKHOUSE_CLIENT} --query "SELECT c1, _row_number FROM url('${URL_CSV}', CSV, 'c1 UInt32, c2 String') ORDER BY c1 SETTINGS input_format_parallel_parsing = 1"

echo "--- URL: _row_number only, parallel_parsing=0"
${CLICKHOUSE_CLIENT} --query "SELECT _row_number FROM url('${URL_CSV}', CSV, 'c1 UInt32, c2 String') ORDER BY _row_number SETTINGS input_format_parallel_parsing = 0"
echo "--- URL: _row_number only, parallel_parsing=1"
${CLICKHOUSE_CLIENT} --query "SELECT _row_number FROM url('${URL_CSV}', CSV, 'c1 UInt32, c2 String') ORDER BY _row_number SETTINGS input_format_parallel_parsing = 1"
