#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Whether a materialized view attached mid-query is fed by that query depends on the route taken,
# not on VALUES vs INSERT ... SELECT:
#
# * Sync route: freezes the destination's dependency graph before the write starts. A view
#   attached later gets nothing from this query.
# * Async queue route: the background flush rebuilds its interpreter from the catalog at flush
#   time, so a view attached between push and flush is picked up. True for plain VALUES inserts
#   under `async_insert = 1` already; single-block INSERT ... SELECT shares the same queue and
#   behavior.
#
# Case 1 pins the frozen-graph sync behavior as a control. Cases 2 and 3 pin that the queue route's
# late-bound behavior is identical for VALUES and for single-block INSERT ... SELECT.

wait_for_async_insert_queue_entry()
{
    local table="$1"
    local timeout="${2:-30}"
    local start=$EPOCHSECONDS
    while [[ $(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.asynchronous_inserts
        WHERE database = currentDatabase() AND table = '$table'
    ") == 0 ]]; do
        if ((EPOCHSECONDS - start > timeout)); then
            echo "Timeout waiting for an async insert queue entry for $table" >&2
            exit 1
        fi
        sleep 0.1
    done
}

# Case 1: a view created while a multi-block (sync fallback) INSERT ... SELECT is still running
# gets zero rows from it, even though the destination table gets the full result.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_mv_race_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_mv_race_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS test_async_sel_mv_race_mv"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_mv_race_dst (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_mv_race_target (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --max_block_size=1000 --async_insert=1 --wait_for_async_insert=1 --query_id insert_case1_${CLICKHOUSE_DATABASE} -q "
    INSERT INTO test_async_sel_mv_race_dst
    SELECT number AS a, number * 2 AS b
    FROM numbers(2000)
    WHERE sleepEachRow(0.001) = 0
" &
INSERT_PID=$!
wait_for_query_to_start "insert_case1_${CLICKHOUSE_DATABASE}" 30
${CLICKHOUSE_CLIENT} -q "
    CREATE MATERIALIZED VIEW test_async_sel_mv_race_mv TO test_async_sel_mv_race_target AS
    SELECT * FROM test_async_sel_mv_race_dst
"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_mv_race_dst"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_mv_race_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW test_async_sel_mv_race_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_mv_race_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_mv_race_dst"

# Case 2: a single-block INSERT ... SELECT that reaches the async insert queue. Busy timeout is
# pinned huge so only the explicit `SYSTEM FLUSH ASYNC INSERT QUEUE` below triggers the flush.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_late_bound_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_late_bound_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS test_async_sel_late_bound_mv"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_late_bound_dst (n UInt64)
    ENGINE = MergeTree ORDER BY n
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_late_bound_target (n UInt64)
    ENGINE = MergeTree ORDER BY n
"
${CLICKHOUSE_CLIENT} \
    --async_insert=1 --async_insert_use_adaptive_busy_timeout=0 \
    --async_insert_busy_timeout_min_ms=600000 --async_insert_busy_timeout_max_ms=600000 \
    --query_id insert_case2_${CLICKHOUSE_DATABASE} -q "
    INSERT INTO test_async_sel_late_bound_dst SELECT number AS n FROM numbers(3)
" &
INSERT_PID=$!
wait_for_async_insert_queue_entry test_async_sel_late_bound_dst 30
${CLICKHOUSE_CLIENT} -q "
    CREATE MATERIALIZED VIEW test_async_sel_late_bound_mv TO test_async_sel_late_bound_target AS
    SELECT * FROM test_async_sel_late_bound_dst
"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE test_async_sel_late_bound_dst"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_late_bound_dst"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_late_bound_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW test_async_sel_late_bound_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_late_bound_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_late_bound_dst"

# Case 3: same shape as case 2, but a plain VALUES insert. Shows the queue route's late-bound
# behavior predates the single-block SELECT feature.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_values_late_bound_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_values_late_bound_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS test_async_values_late_bound_mv"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_values_late_bound_dst (n UInt64)
    ENGINE = MergeTree ORDER BY n
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_values_late_bound_target (n UInt64)
    ENGINE = MergeTree ORDER BY n
"
${CLICKHOUSE_CLIENT} \
    --async_insert=1 --wait_for_async_insert=1 --async_insert_use_adaptive_busy_timeout=0 \
    --async_insert_busy_timeout_min_ms=600000 --async_insert_busy_timeout_max_ms=600000 \
    --query_id insert_case3_${CLICKHOUSE_DATABASE} -q "
    INSERT INTO test_async_values_late_bound_dst VALUES (1), (2), (3)
" &
INSERT_PID=$!
wait_for_async_insert_queue_entry test_async_values_late_bound_dst 30
${CLICKHOUSE_CLIENT} -q "
    CREATE MATERIALIZED VIEW test_async_values_late_bound_mv TO test_async_values_late_bound_target AS
    SELECT * FROM test_async_values_late_bound_dst
"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE test_async_values_late_bound_dst"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_values_late_bound_dst"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_values_late_bound_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW test_async_values_late_bound_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_values_late_bound_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_values_late_bound_dst"
