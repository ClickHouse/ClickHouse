#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
# Tag no-replicated-database: Fails due to additional replicas or shards
# Tag no-shared-merge-tree: No quorum
# Tag no-async-insert: async inserts are not supported with non-parallel quorum inserts

# A non-parallel quorum insert (insert_quorum >= 2, insert_quorum_parallel = 0) permits a single
# in-flight quorum part per table. The max_insert_threads fan-out of such an insert is kept
# single-stream, but the single sink stream is still duplicated into one branch per dependent
# materialized view, and with parallel_view_processing = 1 those branches ran concurrently: two views
# converging on one ReplicatedMergeTree target raced two in-flight quorum parts of one INSERT against
# each other. The views of such an insert must be pushed sequentially, and the INSERT SELECT fan-out
# must stay single-stream as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_views_mv_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_views_mv_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_views_source"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_views_target_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_views_target_2"

$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_views_target_1 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04817/quorum_views_target', '1') ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_views_target_2 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04817/quorum_views_target', '2') ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_views_source (x UInt32) ENGINE = Null"

# Two materialized views converging on the same replicated target table: one INSERT into the source
# writes two quorum parts into the target, one per view branch.
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW quorum_views_mv_1 TO quorum_views_target_1 AS SELECT x FROM quorum_views_source"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW quorum_views_mv_2 TO quorum_views_target_1 AS SELECT x + 100 AS x FROM quorum_views_source"

# A non-parallel quorum INSERT SELECT stays single-stream: one sink.
$CLICKHOUSE_CLIENT $SETTINGS --insert_quorum=2 --insert_quorum_parallel=0 -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_views_target_2 SELECT number FROM numbers(4)" | grep -c "ReplicatedMergeTreeSink"

# A parallel quorum INSERT SELECT tracks every part separately, so the write fan-out applies: four sinks.
$CLICKHOUSE_CLIENT $SETTINGS --insert_quorum=2 --insert_quorum_parallel=1 -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_views_target_2 SELECT number FROM numbers(4)" | grep -c "ReplicatedMergeTreeSink"

# Non-parallel quorum inserts through two views converging on one target succeed with
# parallel_view_processing enabled: the view branches are pushed sequentially, so each branch's
# quorum is satisfied before the next branch commits its part.
for x in 1 2 3 4 5 6; do
    $CLICKHOUSE_CLIENT $SETTINGS --insert_quorum=2 --insert_quorum_parallel=0 --parallel_view_processing=1 --insert_keeper_fault_injection_probability=0 -q \
        "INSERT INTO quorum_views_source VALUES ($x)"
done

$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x), min(x), max(x) FROM quorum_views_target_1"
$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x), min(x), max(x) FROM quorum_views_target_2"

$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_views_mv_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_views_mv_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_views_source"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_views_target_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_views_target_2"
