#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Check that we have memory_usage in progress and summary
CURL_OUTPUT="$(
    ${CLICKHOUSE_CURL} -s -S -v -N -G "${CLICKHOUSE_URL}" \
    --data-urlencode "query=SELECT number, avg(number) FROM numbers(1e6) GROUP BY number FORMAT Null" \
    --data-urlencode "send_progress_in_http_headers=1" \
    --data-urlencode "http_headers_progress_interval_ms=10" \
    --data-urlencode "max_execution_speed=200000" \
    --data-urlencode "timeout_before_checking_execution_speed=0" 2>&1
)"

echo "$CURL_OUTPUT" | grep -m1 'X-ClickHouse-Progress:' | grep -q '"memory_usage":"[1-9][0-9]*"' && echo "Ok"
echo "$CURL_OUTPUT" | grep 'X-ClickHouse-Summary:' | grep -q '"memory_usage":"[1-9][0-9]*"' && echo "Ok"

# Check that we have memory_usage in summary without progress headers
CURL_OUTPUT="$(
    ${CLICKHOUSE_CURL} -s -S -v -N -G "${CLICKHOUSE_URL}" \
        --data-urlencode "query=SELECT number, avg(number) FROM numbers(1e5) GROUP BY number FORMAT Null" \
        --data-urlencode "send_progress_in_http_headers=0" 2>&1
)"

echo "$CURL_OUTPUT" | grep -m1 'X-ClickHouse-Summary:' | grep -q '"memory_usage":"[1-9][0-9]*"' && echo "Ok"

# Check that we have memory_usage in summary for 241 OOM errors.
# Pin `max_threads=1` so the server-level `additional_memory_tracking_per_thread`
# speculative reservation (4 MiB by default) is a single fixed offset. With many
# pipeline workers the cumulative reservation alone exceeds the tight
# `max_memory_usage=20000000` cap and the query fails at reservation time with a
# reported `memory_usage` of 0; with one worker the real aggregation grows past the
# cap and `memory_usage` is reported non-zero as the test expects.
CURL_OUTPUT="$(
    ${CLICKHOUSE_CURL} -s -S -v -N -G "${CLICKHOUSE_URL}" \
    --data-urlencode "query=SELECT number, avg(number) FROM numbers(1e12) GROUP BY number FORMAT Null SETTINGS max_rows_to_read = 0" \
    --data-urlencode "max_memory_usage=20000000" \
    --data-urlencode "max_threads=1" \
    --data-urlencode "max_bytes_before_external_group_by=0" \
    --data-urlencode "max_bytes_ratio_before_external_group_by=0" 2>&1
)"

echo "$CURL_OUTPUT" | grep 'X-ClickHouse-Summary:' | grep -q '"memory_usage":"[1-9][0-9]*"' && echo "Ok"
echo "$CURL_OUTPUT" | grep -q 'X-ClickHouse-Exception-Code: 241' && echo "Ok"
