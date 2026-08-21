#!/usr/bin/env bash
# Tags: no-fasttest
# X-ClickHouse-Summary must not be all-zero for async inserts that don't wait for the flush.
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
    | sed 's/\"accepted_bytes\":\"[1-9][0-9]*\"/\"accepted_bytes\":\"positive\"/'

echo 'waited:'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1" \
    -d "INSERT INTO t_async_summary VALUES (5), (6), (7), (8)" 2>&1 \
    | grep "X-ClickHouse-Summary" | grep -v "Access-Control-Expose-Headers" | sed 's/,\"elapsed_ns[^}]*//' | sed 's/,\"memory_usage[^}]*//' \
    | sed 's/\"accepted_bytes\":\"[1-9][0-9]*\"/\"accepted_bytes\":\"positive\"/'

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE"
${CLICKHOUSE_CLIENT} --query "SELECT 'total_rows', count() FROM t_async_summary"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_async_summary"
