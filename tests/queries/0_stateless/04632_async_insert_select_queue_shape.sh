#!/usr/bin/env bash
# Tags: no-object-storage, no-parallel, no-fasttest
# no-object-storage: object storage adds extra threads, throwing off the peak_threads_usage check
# no-parallel: peak_threads_usage can be lowered by other concurrently running queries

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Coverage for the additive queue transform in InterpreterInsertQuery::addInsertToSelectPipeline:
# a bulk (multi-block) result passes through the normal parallel pipeline, a single small block
# goes to the async queue, and a zero-row result pushes nothing and creates no part.

# Case 1: a bulk INSERT ... SELECT under async_insert=1 must still use the normal, parallel
# pipeline: max_insert_threads and parallel_view_processing take effect, and the query does not
# appear in asynchronous_insert_log. Before this change, the sync fallback reused a dependency
# graph built with max_insert_threads hardcoded to 1, which serialized the insert and any
# materialized view onto a single thread regardless of these settings.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04653_bulk_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04653_bulk_mv_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS test_04653_bulk_mv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04653_bulk_dst (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04653_bulk_mv_target (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "
    CREATE MATERIALIZED VIEW test_04653_bulk_mv TO test_04653_bulk_mv_target AS
    SELECT n FROM test_04653_bulk_dst
"

QUERY_ID="test_04653_bulk_$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="$QUERY_ID" --async_insert=1 --wait_for_async_insert=1 \
    --max_insert_threads=8 --parallel_view_processing=1 --max_block_size=10000 -q "
    INSERT INTO test_04653_bulk_dst SELECT number FROM numbers_mt(200000)
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04653_bulk_dst"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04653_bulk_mv_target"

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
      AND table = 'test_04653_bulk_dst'
"
${CLICKHOUSE_CLIENT} -q "DROP VIEW test_04653_bulk_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04653_bulk_mv_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04653_bulk_dst"

# Case 2: a single small block goes through the async queue (asynchronous_insert_log gets an
# entry), and the row lands.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04653_single"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04653_single (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_04653_single SELECT number FROM numbers(3)
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04653_single"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() >= 1
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04653_single'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04653_single"

# Case 3: a zero-row SELECT pushes nothing to the queue and creates no part.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04653_empty"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04653_empty (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_04653_empty SELECT number FROM numbers(0)
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04653_empty"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_04653_empty' AND active"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04653_empty'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04653_empty"
