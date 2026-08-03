#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_URL="${CLICKHOUSE_URL}&http_wait_end_of_query=1"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' \
    -d 'DROP TABLE IF EXISTS insert_number_table'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' \
    -d 'CREATE TABLE insert_number_table (record UInt32) Engine = Memory'

query_id=$(
    ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=1&http_headers_progress_interval_ms=10&send_progress_in_http_headers=1" \
    -d 'INSERT INTO insert_number_table (record) SELECT number FROM system.numbers LIMIT 10' 2>&1 \
    | grep -F '< X-ClickHouse-Query-Id:' | sed 's/< X-ClickHouse-Query-Id: //' | tr -d '\n\t\r' | xargs
)

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" \
    -d "system flush logs text_log"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" \
    -d "SELECT message FROM system.text_log WHERE level='Error' AND query_id='${query_id}' AND message LIKE '%Request stream is shared by multiple threads. HTTP keep alive is not possible.%'"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' \
    -d 'DROP TABLE insert_number_table'
