#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-async-insert, no-parallel-replicas, no-s3-storage, no-fasttest
# no-parallel: the server allows at most one MASTER/WORKER THREAD resource at a time,
#   and peak_threads_usage is sensitive to concurrent queries
# no-replicated-database: query_log lookup assumes single-node execution
# no-async-insert: test measures synchronous INSERT pipeline threading
# no-parallel-replicas: parallel replicas alter query plans and slot accounting
# no-s3-storage: S3 I/O threads inflate peak_threads_usage beyond the pipeline thread count
# no-fasttest: workload + CPULeaseAllocation setup not exercised in the fast test path

# Regression test for the CPULeaseAllocation half of
# https://github.com/ClickHouse/ClickHouse/pull/102928 (lazy slot allocation).
#
# When workload CPU scheduling is enabled with `cpu_slot_preemption=1` (default),
# the pipeline uses `CPULeaseAllocation` instead of `ConcurrencyControl::Allocation`.
# Pre-fix, the lease eagerly requested `max_threads` worth of slots from the
# workload scheduler at construction time. For an INSERT with one MV and serial
# view processing, the pipeline only needed ~1 thread of work, but the lease
# reserved 10 slots — same INSERT-slot-waste shape as in #102947, but for the
# preemption-aware path.
#
# Post-fix, `CPULeaseAllocation` honors `setMax`. The constructor requests only
# `initial_max_slots` (master thread = 1) up front, and the PipelineExecutor
# upscaling block grows the ceiling on demand. With lazy allocation enabled
# (default), peak_threads_usage stays small.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RESOURCE="04230_cpu_$$"
WORKLOAD="04230_wl_$$"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS ${WORKLOAD}" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS ${RESOURCE}" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_lease_src" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_lease_mvt" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP VIEW IF EXISTS test_lease_mv" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -nm -q "
    CREATE OR REPLACE RESOURCE ${RESOURCE} (MASTER THREAD, WORKER THREAD);
    CREATE OR REPLACE WORKLOAD ${WORKLOAD};
    DROP TABLE IF EXISTS test_lease_src;
    DROP TABLE IF EXISTS test_lease_mvt;
    DROP VIEW IF EXISTS test_lease_mv;
    CREATE TABLE test_lease_src (x UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE test_lease_mvt (x UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE MATERIALIZED VIEW test_lease_mv TO test_lease_mvt AS SELECT x FROM test_lease_src;
"

QUERY_ID="04230_lease_$RANDOM"

# INSERT with MV through CPULeaseAllocation. The pipeline asks for max_threads
# because views are involved, but the workload scheduler should only hand out
# slots that the pipeline actually consumes. Without lazy CPULeaseAllocation,
# peak_threads_usage would be close to max_threads (10).
$CLICKHOUSE_CLIENT \
    --query_id="$QUERY_ID" \
    --max_threads=10 \
    --max_insert_threads=1 \
    --parallel_view_processing=0 \
    --use_concurrency_control=1 \
    --workload="${WORKLOAD}" \
    --log_queries=1 \
    -q "INSERT INTO test_lease_src SELECT number FROM numbers(1000)"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# Server still alive and lease released cleanly
$CLICKHOUSE_CLIENT -q "SELECT 'server alive'"

# Threshold 4 mirrors 04102 / 04229. Pre-fix peak would be ~10.
$CLICKHOUSE_CLIENT -q "
    SELECT
        if(peak_threads_usage <= 4, 'FEW THREADS', concat('TOO MANY THREADS: ', toString(peak_threads_usage)))
    FROM system.query_log
    WHERE event_date >= yesterday()
        AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id = '$QUERY_ID'
    SETTINGS optimize_if_transform_strings_to_enum = 0
"
