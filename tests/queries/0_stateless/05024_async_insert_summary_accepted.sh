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

echo 'zero-row query still reports its final memory_usage progress header:'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&max_untracked_memory=1" \
    -d "SELECT * FROM numbers(0)" 2>&1 \
    | grep -q -E '^< X-ClickHouse-Progress: \{("elapsed_ns":"[0-9]+",)?"memory_usage":"[1-9][0-9]*"\}' && echo 1 || echo 0

echo 'empty fire-and-forget must not emit a counterless progress header:'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&query=INSERT%20INTO%20t_async_summary%20FORMAT%20JSONEachRow" \
    --data-binary '' 2>&1 | grep '^< X-ClickHouse-Progress:' | grep -c -E ':[[:space:]]*\{\}|accepted' || true

echo 'low-priority fire-and-forget still reports accepted_bytes and is not preempted after enqueue:'
high_priority_query_id="high_priority_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id="$high_priority_query_id" --priority=1 --query "SELECT sleep(3) FORMAT Null" &
for _ in $(seq 1 100); do
    [[ $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$high_priority_query_id'") == 1 ]] && break
    sleep 0.1
done
insert_query_id="low_priority_insert_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&priority=2&query_id=$insert_query_id" \
    -d "INSERT INTO t_async_summary VALUES (13), (14)" 2>&1 \
    | grep "X-ClickHouse-Summary" | grep -v "Access-Control-Expose-Headers" | grep -c '"accepted_bytes":"[1-9][0-9]*"'
wait
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    preempted=$(${CLICKHOUSE_CLIENT} --query "SELECT ProfileEvents['QueryPreempted'] FROM system.query_log WHERE event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$insert_query_id' AND type = 'QueryFinish'")
    [[ -n "$preempted" ]] && break
    sleep 0.5
done
echo "QueryPreempted: ${preempted:-missing}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE t_async_summary"
${CLICKHOUSE_CLIENT} --query "SELECT 'total_rows', count() FROM t_async_summary"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_async_summary"
