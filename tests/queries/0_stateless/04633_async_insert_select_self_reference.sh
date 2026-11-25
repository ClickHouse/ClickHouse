#!/usr/bin/env bash
# Checks table names in asynchronous_insert_log; no thread/topology dependence, so no tags needed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A self referencing `INSERT ... SELECT` must not take the async insert queue route: the SELECT
# side's own lock would outlive the queue's flush wait and can deadlock with a concurrent exclusive
# lock. Settings are pinned per query so the randomizer cannot split the block into several.

# Case 1: self referencing INSERT ... SELECT must not reach the async queue, even though the shape
# (MergeTree destination, one small block, no views) would otherwise qualify.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_self"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_self (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_04633_self SETTINGS async_insert = 0 VALUES (1), (2), (3)"
query_id_case1="${CLICKHOUSE_DATABASE}_04633_case1"
${CLICKHOUSE_CLIENT} --query_id "$query_id_case1" -q "
    INSERT INTO test_04633_self SELECT n FROM test_04633_self
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             max_threads = 1, max_insert_threads = 1,
             max_block_size = 1000, preferred_block_size_bytes = 0
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04633_self"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
# Filtered by query_id, not just table: the AsyncInsert CI profile forces async_insert = 1 for the
# seed insert too, which would otherwise leave its own row in the log.
${CLICKHOUSE_CLIENT} -q "
    SELECT count() = 0
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04633_self'
      AND query_id = '$query_id_case1'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_self"

# Case 2: same query shape against a different source table is not self referencing, so it does
# take the async queue route. Positive control showing case 1's shape would qualify otherwise.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_other_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_other_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_other_dst (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_other_src (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_04633_other_src SETTINGS async_insert = 0 VALUES (1), (2), (3)"
query_id_case2="${CLICKHOUSE_DATABASE}_04633_case2"
${CLICKHOUSE_CLIENT} --query_id "$query_id_case2" -q "
    INSERT INTO test_04633_other_dst SELECT n FROM test_04633_other_src
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             max_threads = 1, max_insert_threads = 1,
             max_block_size = 1000, preferred_block_size_bytes = 0
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04633_other_dst"
# Log element is queued after the query returns, so one flush can miss it:
# https://github.com/ClickHouse/ClickHouse/issues/84364. Retry bounded so a real failure prints 0.
# Filtered by query_id so only the INSERT ... SELECT under test is seen, not the seed insert.
reached_queue=0
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    reached_queue=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() >= 1
        FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND database = currentDatabase()
          AND table = 'test_04633_other_dst'
          AND query_id = '$query_id_case2'
    ")
    [ "$reached_queue" = 1 ] && break
    sleep 0.5
done
echo "$reached_queue"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_other_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_other_src"
