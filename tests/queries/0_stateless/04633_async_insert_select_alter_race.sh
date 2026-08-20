#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Concurrent DDL against a running INSERT ... SELECT under async_insert. Split out of
# 04633_async_insert_select_regression to keep each long-running race in its own test.
# Case numbers kept from that test.
#
# Each insert sleeps per row so the ALTER lands mid-SELECT, pinning that the pipeline keeps using
# the schema frozen before the SELECT started.

# Case 3: concurrent MODIFY COLUMN FIRST must not corrupt data (MatchColumnsMode::Name regression).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_alter_race"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_alter_race (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --max_block_size=1000 --async_insert=1 --wait_for_async_insert=1 --query_id insert_case3_${CLICKHOUSE_DATABASE} -q "
    INSERT INTO test_async_sel_alter_race
    SELECT number AS a, number * 2 AS b
    FROM numbers(2000)
    WHERE sleepEachRow(0.001) = 0
" &
INSERT_PID=$!
wait_for_query_to_start "insert_case3_${CLICKHOUSE_DATABASE}" 30
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_alter_race MODIFY COLUMN b UInt32 FIRST"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_alter_race WHERE b != a * 2"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_alter_race"

# Case 4: concurrent ADD COLUMN must not cause THERE_IS_NO_COLUMN (schema freeze, async path).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_add_col_single"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_add_col_single (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --optimize_trivial_insert_select=1 --async_insert=1 --wait_for_async_insert=1 --query_id insert_case4_${CLICKHOUSE_DATABASE} -q "
    INSERT INTO test_async_sel_add_col_single
    SELECT number AS a, number * 2 AS b
    FROM numbers(2000)
    WHERE sleepEachRow(0.001) = 0
" &
INSERT_PID=$!
wait_for_query_to_start "insert_case4_${CLICKHOUSE_DATABASE}" 30
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_add_col_single ADD COLUMN c UInt32 DEFAULT 42"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_single"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_single WHERE b = a * 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_single WHERE c = 42"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_add_col_single"

# Case 5: same ADD COLUMN race on the sync-fallback path (schema freeze, multi-block).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_add_col_multi"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_add_col_multi (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --max_block_size=1000 --async_insert=1 --wait_for_async_insert=1 --query_id insert_case5_${CLICKHOUSE_DATABASE} -q "
    INSERT INTO test_async_sel_add_col_multi
    SELECT number AS a, number * 2 AS b
    FROM numbers(2000)
    WHERE sleepEachRow(0.001) = 0
" &
INSERT_PID=$!
wait_for_query_to_start "insert_case5_${CLICKHOUSE_DATABASE}" 30
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_add_col_multi ADD COLUMN c UInt32 DEFAULT 42"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_multi"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_multi WHERE b = a * 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_multi WHERE c = 42"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_add_col_multi"

# Case 6: a writer queued on the destination while the SELECT still reads must not deadlock with the
# queue flush. The flush relocks the table under its own query id, so it cannot join the reader group
# of this query: a share lock kept across the flush wait leaves the writer between the two and
# neither side moves. `POPULATE` locks its source for write, unlike ALTER (alter lock only) or
# TRUNCATE (no table lock at all on MergeTree).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_excl_lock_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_excl_lock"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_excl_lock (a UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --optimize_trivial_insert_select=1 --async_insert=1 --wait_for_async_insert=1 \
    --query_id insert_case6_${CLICKHOUSE_DATABASE} -q "
    INSERT INTO test_async_sel_excl_lock
    SELECT number AS a
    FROM numbers(2000)
    WHERE sleepEachRow(0.001) = 0
" &
INSERT_PID=$!
wait_for_query_to_start "insert_case6_${CLICKHOUSE_DATABASE}" 30
# Waits out the SELECT above, then takes the write lock while the INSERT waits for its flush. Short
# timeout: it bounds what the deadlock costs here, and the good path needs only the SELECT to end.
${CLICKHOUSE_CLIENT} --lock_acquire_timeout=10 -q "
    CREATE MATERIALIZED VIEW test_async_sel_excl_lock_mv
    ENGINE = MergeTree ORDER BY a POPULATE
    AS SELECT a FROM test_async_sel_excl_lock
"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_excl_lock"
# The view's row count is left unchecked: the lock queue, not this script, orders the population
# against the flush.
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_excl_lock_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_excl_lock"

# Case 7: no share lock may outlive the query either. `wait_for_async_insert = 1` returns only after
# the flush, so a writer right behind it has nothing left to wait for.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_lock_leak"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_lock_leak_renamed"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_lock_leak (a UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --optimize_trivial_insert_select=1 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_lock_leak SELECT number AS a FROM numbers(2000)
"
# Short timeout, so a leaked share lock fails here instead of stalling the test for two minutes.
${CLICKHOUSE_CLIENT} --lock_acquire_timeout=10 -q "
    RENAME TABLE test_async_sel_lock_leak TO test_async_sel_lock_leak_renamed
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_lock_leak_renamed"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_lock_leak_renamed"
