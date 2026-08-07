#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t0";
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t0 (x UInt64, val String) ENGINE = MergeTree ORDER BY x PARTITION BY x % 100";
${CLICKHOUSE_CLIENT} -q "INSERT INTO t0 SELECT sipHash64(number), randomPrintableASCII(1000) FROM numbers(1000)";


# Check that we have at least 3 different values for read_rows
UNIQUE_VALUES=$(
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d @- <<< "CHECK TABLE t0" -v |& {
        grep -F -e X-ClickHouse-Progress: -e X-ClickHouse-Summary:  | grep -o '"read_rows"\s*:\s*"[0-9]*"'
    } | uniq | wc -l
)

[ "$UNIQUE_VALUES" -ge "3" ] && echo "Ok" || echo "Fail: got $UNIQUE_VALUES"


# Check that we have we have at least 100 total_rows_to_read (at least one check task per partition)
MAX_TOTAL_VALUE=$(
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d @- <<< "CHECK TABLE t0" -v |& {
        grep -F -e X-ClickHouse-Progress: -e X-ClickHouse-Summary:  | grep -o '"total_rows_to_read"\s*:\s*"[0-9]*"' | grep -o '[0-9]*'
    } | sort -n | tail -1
)

[ "$MAX_TOTAL_VALUE" -ge "100" ] && echo "Ok" || echo "Fail: got $MAX_TOTAL_VALUE"
