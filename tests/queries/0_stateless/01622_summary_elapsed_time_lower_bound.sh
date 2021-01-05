#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -d 'DROP TABLE IF EXISTS insert_number_query' > /dev/null 2>&1
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -d 'CREATE TABLE insert_number_query (record UInt32) Engine = Memory' > /dev/null 2>&1

rows=100
sleep_each_row=0.0123
expected_elapsed_time=$(echo "$rows * $sleep_each_row * 1000000000" | bc -l) # nanoseconds
elapsed_time=$(
    ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1" -d "INSERT INTO insert_number_query (record) SELECT sleepEachRow($sleep_each_row) FROM numbers($rows)" 2>&1 | grep -E 'X-ClickHouse-Summary' | sed -r -e 's/^.*"elapsed_time":"([0-9]+)".*$/\1/'
)

if [ $(echo "$elapsed_time < $expected_elapsed_time" | bc -l) -eq "1" ] ; then
    echo "FAIL : actual elapsed time ($elapsed_time) < expected elapsed time ($expected_elapsed_time)"
else
    echo "OK : actual elapsed time >= expected elapsed time ($expected_elapsed_time)"
fi
