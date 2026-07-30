#!/usr/bin/env bash
# Tags: no-object-storage, no-parallel, no-fasttest
# no-object-storage: object storage adds extra threads, throwing off the peak_threads_usage check
# no-parallel: peak_threads_usage can be lowered by other concurrently running queries

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Coverage for the additive queue transform in InterpreterInsertQuery::addInsertToSelectPipeline:
# which result shapes reach the async queue and which pass through to the normal pipeline.
# Settings that decide a shape are passed per query, not per session, so the test settings
# randomizer cannot rewrite them.

# Case 1: a bulk INSERT ... SELECT under async_insert=1 keeps the normal, parallel pipeline.
# The destination has no dependent view, so the transform is added and the second block is what
# makes it pass through. Before this change the fallback reused a dependency graph built with
# max_insert_threads hardcoded to 1, which serialized the insert regardless of the setting.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04632_bulk_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04632_bulk_dst (n UInt64) ENGINE = MergeTree ORDER BY n"

QUERY_ID="test_04632_bulk_$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="$QUERY_ID" -q "
    INSERT INTO test_04632_bulk_dst SELECT number FROM numbers_mt(200000)
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             max_threads = 8, max_insert_threads = 8, max_block_size = 10000
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04632_bulk_dst"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log, asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT if(peak_threads_usage >= 2, 'parallel', 'serial')
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND query_id = '$QUERY_ID'
"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04632_bulk_dst'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04632_bulk_dst"

# Case 2: a destination with a dependent materialized view never reaches the queue, whatever the
# result shape, because a view target can be any engine. parallel_view_processing still applies.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04632_mv_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04632_mv_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS test_04632_mv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04632_mv_dst (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04632_mv_target (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "
    CREATE MATERIALIZED VIEW test_04632_mv TO test_04632_mv_target AS
    SELECT n FROM test_04632_mv_dst
"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_04632_mv_dst SELECT number FROM numbers(3)
    SETTINGS async_insert = 1, wait_for_async_insert = 1, parallel_view_processing = 1
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04632_mv_dst"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04632_mv_target"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04632_mv_dst'
"
${CLICKHOUSE_CLIENT} -q "DROP VIEW test_04632_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04632_mv_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04632_mv_dst"

# Case 3: a single small block goes through the async queue and the rows land.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04632_single"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04632_single (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_04632_single SELECT number FROM numbers(3)
    SETTINGS async_insert = 1, wait_for_async_insert = 1
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04632_single"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() >= 1
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04632_single'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04632_single"

# Case 4: a zero-row SELECT pushes nothing to the queue and creates no part.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04632_empty"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04632_empty (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_04632_empty SELECT number FROM numbers(0)
    SETTINGS async_insert = 1, wait_for_async_insert = 1
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04632_empty"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_04632_empty' AND active"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04632_empty'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04632_empty"

# Cases 5 to 7: a single block whose size only shows up once expanded must not be queued, and must
# not be expanded to find that out. Each result is one block of 10000 rows carrying a 1 KiB value,
# roughly 10 MiB expanded, against async_insert_max_data_size = 100000. The assertions pin the
# invariant (no queue entry, every row lands) without depending on which column representation the
# SELECT produces, so they hold whether the value arrives const, sparse, replicated or already full.

# Case 5: constant value repeated over the block.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04632_const"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04632_const (s String) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_04632_const SELECT repeat('x', 1024) FROM numbers(10000)
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             async_insert_max_data_size = 100000, max_block_size = 100000
"
${CLICKHOUSE_CLIENT} -q "SELECT count(), any(length(s)) FROM test_04632_const"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04632_const'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04632_const"

# Case 6: the same value inside a tuple, which removeSpecialRepresentations expands per element.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04632_tuple"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04632_tuple (t Tuple(String, UInt64)) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_04632_tuple SELECT (repeat('y', 1024), number) FROM numbers(10000)
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             async_insert_max_data_size = 100000, max_block_size = 100000
"
${CLICKHOUSE_CLIENT} -q "SELECT count(), any(length(t.1)) FROM test_04632_tuple"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04632_tuple'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04632_tuple"

# Case 7: the value arrives through a JOIN, which can hand the pipeline a lazy replicated column.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04632_join"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04632_join (s String) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_04632_join
    SELECT r.s FROM numbers(10000) AS l
    JOIN (SELECT 0 AS k, repeat('z', 1024) AS s) AS r ON l.number % 1 = r.k
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             async_insert_max_data_size = 100000, max_block_size = 100000
"
${CLICKHOUSE_CLIENT} -q "SELECT count(), any(length(s)) FROM test_04632_join"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04632_join'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04632_join"
