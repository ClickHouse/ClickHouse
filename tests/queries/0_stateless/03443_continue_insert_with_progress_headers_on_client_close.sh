#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'CREATE TABLE numbers (x int) ENGINE = Memory();'

# Query takes 10 seconds and sends back progress headers, we close the connection after 5 seconds and expect the query to continue
${CLICKHOUSE_CURL} --max-time 5 -sS "${CLICKHOUSE_URL}&query_id=continue_insert_on_network_issue&max_threads=1&send_progress_in_http_headers=1" --data-binary $'INSERT INTO numbers SELECT sleepEachRow(0.1) + number as x from system.numbers limit 100 settings function_sleep_max_microseconds_per_block = 0, send_progress_in_http_headers = 1, insert_deduplicate = 0, max_block_size = 10;'  2>&1 | grep -cF 'curl: (28)'

sleep 5
wait_for_queries_to_finish 40

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT count() FROM numbers;'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'DROP TABLE numbers;'
