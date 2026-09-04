#!/usr/bin/env bash
# Tags: no-fasttest
# https://github.com/ClickHouse/ClickHouse/issues/57768

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_async_summary"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_async_summary (x UInt64) ENGINE = MergeTree ORDER BY x"

echo 'fire-and-forget:'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0" \
    -d "INSERT INTO t_async_summary VALUES (1), (2), (3), (4)" 2>&1 \
    | grep "X-ClickHouse-Summary" | grep -v "Access-Control-Expose-Headers" | sed 's/,\"elapsed_ns[^}]*//' | sed 's/,\"memory_usage[^}]*//' \
    | sed -E 's/"(read_bytes|written_bytes|result_bytes|accepted_bytes)":"[1-9][0-9]*"/"\1":"positive"/g'

echo 'waited:'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1" \
    -d "INSERT INTO t_async_summary VALUES (5), (6), (7), (8)" 2>&1 \
    | grep "X-ClickHouse-Summary" | grep -v "Access-Control-Expose-Headers" | sed 's/,\"elapsed_ns[^}]*//' | sed 's/,\"memory_usage[^}]*//' \
    | sed -E 's/"(read_bytes|written_bytes|result_bytes|accepted_bytes)":"[1-9][0-9]*"/"\1":"positive"/g'

echo 'progress headers must not contain accepted fields:'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=1" \
    -d "INSERT INTO t_async_summary VALUES (9), (10)" 2>&1 \
    | grep "X-ClickHouse-Progress" | grep -c "accepted" || true

echo 'progress headers must not be emitted empty by the accepted-only update:'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" \
    -d "INSERT INTO t_async_summary VALUES (11), (12)" 2>&1 \
    | grep "X-ClickHouse-Progress" | grep -c -E ':[[:space:]]*\{\}|:[[:space:]]*\{"memory_usage":"[0-9]+"\}' || true

echo 'empty fire-and-forget must not emit progress headers:'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&query=INSERT%20INTO%20t_async_summary%20FORMAT%20JSONEachRow" \
    --data-binary '' 2>&1 | grep -c '^< X-ClickHouse-Progress:' || true

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE t_async_summary"
${CLICKHOUSE_CLIENT} --query "SELECT 'total_rows', count() FROM t_async_summary"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_async_summary"
