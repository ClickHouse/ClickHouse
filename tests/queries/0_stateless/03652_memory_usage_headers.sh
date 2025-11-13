#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Check that we have memory_usage in progress and summary
CURL_OUTPUT="$(
    ${CLICKHOUSE_CURL} -s -S -v -N -G "${CLICKHOUSE_URL}" \
    --data-urlencode "query=SELECT number, avg(number) FROM numbers(1e7) GROUP BY number FORMAT Null" \
    --data-urlencode "cancel_http_readonly_queries_on_client_close=1" \
    --data-urlencode "send_progress_in_http_headers=1" \
    --data-urlencode "http_headers_progress_interval_ms=10" 2>&1
)"

echo "$CURL_OUTPUT" | grep -m1 'X-ClickHouse-Progress:' | grep -q '"memory_usage":"[1-9][0-9]*"' && echo "Ok"
echo "$CURL_OUTPUT" | grep 'X-ClickHouse-Summary:' | grep -q '"memory_usage":"[1-9][0-9]*"' && echo "Ok"

# Check that we have memory_usage in summary without progress headers
${CLICKHOUSE_CURL} -s -S -v -N -G "${CLICKHOUSE_URL}" \
    --data-urlencode "query=SELECT number, avg(number) FROM numbers(1e5) GROUP BY number FORMAT Null" \
    --data-urlencode "cancel_http_readonly_queries_on_client_close=1" \
    --data-urlencode "send_progress_in_http_headers=0" 2>&1 |
grep -m1 'X-ClickHouse-Summary:' |
grep -q '"memory_usage":"[1-9][0-9]*"' && echo "Ok"

# Check that we have memory_usage in summary for 241 OOM errors
CURL_OUTPUT="$(
    ${CLICKHOUSE_CURL} -s -S -v -N -G "${CLICKHOUSE_URL}" \
    --data-urlencode "query=SELECT number, avg(number) FROM numbers(1e12) GROUP BY number FORMAT Null SETTINGS max_rows_to_read = 0" \
    --data-urlencode "cancel_http_readonly_queries_on_client_close=1" \
    --data-urlencode "send_progress_in_http_headers=1" \
    --data-urlencode "max_memory_usage=100000000" 2>&1
)"

echo "$CURL_OUTPUT" | grep 'X-ClickHouse-Summary:' | grep -q '"memory_usage":"[1-9][0-9]*"' && echo "Ok"
echo "$CURL_OUTPUT" | grep -q 'X-ClickHouse-Exception-Code: 241' && echo "Ok"
