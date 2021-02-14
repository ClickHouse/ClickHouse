#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -d 'DROP TABLE IF EXISTS insert_number_query' > /dev/null 2>&1
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -d 'CREATE TABLE insert_number_query (record UInt32) Engine = Memory' > /dev/null 2>&1

rows=100
sleep_each_row=0.0123
expected_processing_time=$(python3 -c "print($rows * $sleep_each_row * 1000000000)") # nanoseconds
processing_time=$(
    ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1" -d "INSERT INTO insert_number_query (record) SELECT sleepEachRow($sleep_each_row) FROM numbers($rows)" 2>&1 | grep -E 'X-ClickHouse-Summary' | sed -r -e 's/^.*"cumulative_processing_time":"([0-9]+)".*$/\1/'
)

if python3 -c "import sys; sys.exit(0 if $processing_time >= $expected_processing_time else 1)" ; then
    echo "OK : actual cumulative processing time >= expected cumulative processing time ($expected_processing_time)"
else
    echo "FAIL : actual cumulative processing time ($processing_time) < expected cumulative processing time ($expected_processing_time)"
fi
