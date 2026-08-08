#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
# Tag no-replicated-database: Fails due to additional replicas or shards
# Tag no-shared-merge-tree: No quorum
# Tag no-async-insert: async inserts are not supported with non-parallel quorum inserts

# The serialization a non-parallel quorum insert (insert_quorum >= 2, insert_quorum_parallel = 0)
# needs is derived from the sink graph, not from the settings alone: only writes reaching a
# ReplicatedMergeTree table are quorum writes, and only two of them racing on the same table
# conflict. An insert whose write graph never reaches a replicated table keeps its
# max_insert_threads fan-out even under a global quorum profile, while a replicated table reachable
# behind a dependent materialized view still forces the fan-out down to a single stream. Dependent
# views writing to distinct replicated tables keep running concurrently.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0"
QUORUM_SETTINGS="$SETTINGS --insert_quorum=2 --insert_quorum_parallel=0 --insert_keeper_fault_injection_probability=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_mv_plain"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_mv_a"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_mv_b"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_plain"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_plain_viewed"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_source"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_target_a_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_target_a_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_target_b_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_graph_target_b_2"

$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_graph_plain (x UInt32) ENGINE = MergeTree ORDER BY x"

# A non-parallel quorum INSERT SELECT into a plain MergeTree table never writes a quorum part, so it
# keeps the write fan-out: four sinks.
$CLICKHOUSE_CLIENT $QUORUM_SETTINGS -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_graph_plain SELECT number FROM numbers(4)" | grep -c "MergeTreeSink"

# The same plain destination with a dependent materialized view targeting a ReplicatedMergeTree
# table does produce quorum parts - through the view - so the fan-out drops to a single stream:
# one sink for the destination table plus one for the view target.
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_graph_plain_viewed (x UInt32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_graph_target_a_1 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04823/quorum_graph_target_a', '1') ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_graph_target_a_2 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04823/quorum_graph_target_a', '2') ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW quorum_graph_mv_plain TO quorum_graph_target_a_1 AS SELECT x FROM quorum_graph_plain_viewed"
$CLICKHOUSE_CLIENT $QUORUM_SETTINGS --parallel_view_processing=1 -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_graph_plain_viewed SELECT number FROM numbers(4)" | grep -c "MergeTreeSink"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_mv_plain"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_plain_viewed"

# Two materialized views writing to two distinct replicated tables do not share a quorum node, so
# non-parallel quorum inserts through them succeed with parallel_view_processing enabled.
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_graph_target_b_1 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04823/quorum_graph_target_b', '1') ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_graph_target_b_2 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04823/quorum_graph_target_b', '2') ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_graph_source (x UInt32) ENGINE = Null"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW quorum_graph_mv_a TO quorum_graph_target_a_1 AS SELECT x FROM quorum_graph_source"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW quorum_graph_mv_b TO quorum_graph_target_b_1 AS SELECT x + 100 AS x FROM quorum_graph_source"

for x in 1 2 3; do
    $CLICKHOUSE_CLIENT $QUORUM_SETTINGS --parallel_view_processing=1 -q \
        "INSERT INTO quorum_graph_source VALUES ($x)"
done

$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x) FROM quorum_graph_target_a_1"
$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x) FROM quorum_graph_target_a_2"
$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x) FROM quorum_graph_target_b_1"
$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x) FROM quorum_graph_target_b_2"

$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_mv_a"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_mv_b"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_source"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_plain"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_target_a_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_target_a_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_target_b_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_graph_target_b_2"
