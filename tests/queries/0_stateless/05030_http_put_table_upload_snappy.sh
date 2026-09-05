#!/usr/bin/env bash

set -euo pipefail

# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
TABLE="put_table_05030"
ROUNDTRIP_TABLE="put_table_05030_roundtrip"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.${TABLE}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.${ROUNDTRIP_TABLE}"
}
trap cleanup EXIT

cleanup
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.${TABLE} (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.${ROUNDTRIP_TABLE} (a UInt32, b String) ENGINE=Memory"

echo "===== PUT table upload with Snappy ====="
echo "-- snappy compression suffix decompresses the request body"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compression=snappy&snappy_mode=basic" \
    -d "SELECT concat('17,\"snappy\"', char(10)) FORMAT RawBLOB" \
    | ${CLICKHOUSE_CURL} -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${TABLE}.CSV.snappy"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${TABLE} WHERE a = 17 AND b = 'snappy'" | grep -qx '1'

echo "-- matching path-table reads and uploads use the same default Snappy mode"
${CLICKHOUSE_CURL} -sS \
    "${BASE_URL}/${DB}/${TABLE}.CSV.snappy?http_allow_database_as_path=1&http_allow_table_as_file=1" \
    | ${CLICKHOUSE_CURL} -sS -X PUT -H 'Content-Type: text/csv' --data-binary @- \
        "${BASE_URL}/${DB}/${ROUNDTRIP_TABLE}.CSV.snappy"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.${ROUNDTRIP_TABLE} WHERE a = 17 AND b = 'snappy'" | grep -qx '1'

echo "-- snappy compression suffix supports the framed mode"
${CLICKHOUSE_CURL} -sS \
    "${BASE_URL}/${DB}/${TABLE}.CSV.snappy?http_allow_database_as_path=1&http_allow_table_as_file=1&snappy_mode=framed" \
    | od -A n -t x1 -N 10 \
    | tr -d ' \n' \
    | grep -q '^ff060000734e61507059$'
