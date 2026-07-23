#!/usr/bin/env bash
# Tags: long, no-parallel-replicas
# no-parallel-replicas: STREAM is not supported with parallel replicas.

# A STREAM read over a multi-partition table with a row policy used to abort the server: the
# per-partition read sub-plans shared one row-level-filter ActionsDAG (via query_info), and each
# sub-plan's plan.optimize() mutated it in place, so a later partition optimized a DAG an earlier
# one had already rewritten. In debug/sanitizer builds this aborts with the "Filter column ... not
# found in DAG outputs" logical error (an exception, caught in release builds) or segfaults.
# The abort is timing-dependent, so the query is looped; each run also checks the policy is applied.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP ROW POLICY IF EXISTS rp_04545 ON t_streaming_rp"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_streaming_rp"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_streaming_rp (a UInt32, b UInt32)
    ENGINE = MergeTree PARTITION BY a ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"

# One denied row (b = 9000) per partition, committed FIRST, so it sits at the head of each
# partition's commit-order stream. If the row policy were skipped it would surface in the read
# below and the assertion would fail.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_streaming_rp SELECT number, 9000 FROM numbers(10)"
# Ten partitions of allowed rows (b < 5000), committed second, enough that the bounded STREAM read
# completes from storage without waiting for new inserts.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_streaming_rp SELECT number % 10, number % 5000 FROM numbers(10000)"

# Selective row policy: only b < 5000 is visible. query_info.row_level_filter is a shared
# FilterDAGInfo -- the bug is in that DAG being optimized once per partition.
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY rp_04545 ON t_streaming_rp FOR SELECT USING b < 5000 TO ALL"

for _ in {1..30}; do
    # query_plan_remove_unused_columns = 1 is the optimization that mutates the shared DAG; CI
    # randomizes it, so pin it on. max_threads > 1 is required to hit the race (a single thread
    # serializes the per-partition sub-plan builds); max_threads_min_free_memory_per_thread = 0
    # stops the memory limiter from silently collapsing that back to one thread under CI load.
    # Every visible row must satisfy the policy (b < 5000) and at least one row must be read: this
    # fails loudly (set -e) if the server dies, and returns 0 if the row policy is skipped (a
    # denied b = 9000 row would then head the stream).
    result=$($CLICKHOUSE_CLIENT --enable_streaming_queries=1 --query_plan_remove_unused_columns=1 --max_threads=4 --max_threads_min_free_memory_per_thread=0 --max_execution_time=20 \
        -q "SELECT max(b) < 5000 AND count() > 0 FROM (SELECT b FROM t_streaming_rp STREAM LIMIT 400)")
    if [[ "$result" != "1" ]]; then
        echo "policy check failed: $result"
        exit 1
    fi
done

echo "ok"

$CLICKHOUSE_CLIENT -q "DROP ROW POLICY rp_04545 ON t_streaming_rp"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_streaming_rp"
