#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-async-insert, no-parallel-replicas, no-s3-storage
# no-parallel: checks thread count, which can be affected by concurrent queries
# no-replicated-database: query_log lookup assumes single-node execution
# no-async-insert: test measures synchronous INSERT pipeline threading
# no-parallel-replicas: parallel replicas settings alter query execution plans and thread allocation
# no-s3-storage: S3 I/O threads inflate peak_threads_usage beyond the pipeline thread count

# Verifies that a plain INSERT (no SELECT, no MVs) does not request
# excessive ConcurrencyControl slots or spawn unnecessary threads.
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/102947

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_insert_threads"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_insert_threads_mv"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_insert_threads (x UInt64) ENGINE = MergeTree ORDER BY x"

# Test 1: Plain INSERT FORMAT TSV with max_threads=16, no MVs.
# The insert pipeline is a single chain — should use max_insert_threads (1).
echo "=== Plain INSERT without MVs ==="
QUERY_ID1="04102_no_mv_$RANDOM"

$CLICKHOUSE_CLIENT -q "SELECT number FROM numbers(10000) FORMAT TSV" | \
$CLICKHOUSE_CLIENT \
    --query_id="$QUERY_ID1" \
    --max_threads=16 \
    --max_insert_threads=1 \
    --input_format_parallel_parsing=0 \
    --log_queries=1 \
    -q "INSERT INTO test_insert_threads FORMAT TSV"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "
    SELECT
        if(peak_threads_usage <= 4, 'FEW THREADS', 'MANY THREADS SPAWNED')
    FROM system.query_log
    WHERE event_date >= yesterday()
        AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id = '$QUERY_ID1'
    SETTINGS optimize_if_transform_strings_to_enum = 0
"

# Test 2: INSERT with a materialized view — should use more threads.
echo "=== Plain INSERT with MV ==="
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW test_insert_threads_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM test_insert_threads"

QUERY_ID2="04102_with_mv_$RANDOM"

$CLICKHOUSE_CLIENT -q "SELECT number FROM numbers(10000) FORMAT TSV" | \
$CLICKHOUSE_CLIENT \
    --query_id="$QUERY_ID2" \
    --max_threads=16 \
    --max_insert_threads=1 \
    --input_format_parallel_parsing=0 \
    --log_queries=1 \
    -q "INSERT INTO test_insert_threads FORMAT TSV"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# With MVs, peak_threads should be higher than without MVs
$CLICKHOUSE_CLIENT -q "
    SELECT
        if(peak_threads_usage > 1, 'MORE THAN 1 THREAD', 'SINGLE THREAD')
    FROM system.query_log
    WHERE event_date >= yesterday()
        AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id = '$QUERY_ID2'
    SETTINGS optimize_if_transform_strings_to_enum = 0
"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_insert_threads_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_insert_threads"
