#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
# Tag no-replicated-database: Fails due to additional replicas or shards
# Tag no-shared-merge-tree: No quorum
# Tag no-async-insert: async inserts are not supported with non-parallel quorum inserts

# A non-parallel quorum insert (insert_quorum >= 2, insert_quorum_parallel = 0) permits a single
# in-flight quorum part per table: every ReplicatedMergeTreeSink checks in onStart that the quorum
# of all previous writes is satisfied. With a write fan-out under max_insert_threads, sibling sinks
# of the same INSERT raced against the not-yet-satisfied quorum node of the part committed by the
# branch that got the data and failed with UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE.
# Such inserts must stay single-stream.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_single_stream_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_single_stream_2"

$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_single_stream_1 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04652/quorum_single_stream', '1') ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_single_stream_2 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04652/quorum_single_stream', '2') ORDER BY x"

# A non-parallel quorum insert stays single-stream: one sink.
$CLICKHOUSE_CLIENT $SETTINGS --insert_quorum=2 --insert_quorum_parallel=0 -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_single_stream_1 VALUES (100)" | grep -c "ReplicatedMergeTreeSink"

# A parallel quorum insert tracks every part separately, so the write fan-out applies: four sinks.
$CLICKHOUSE_CLIENT $SETTINGS --insert_quorum=2 --insert_quorum_parallel=1 -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_single_stream_1 VALUES (100)" | grep -c "ReplicatedMergeTreeSink"

# Sequential non-parallel quorum inserts succeed under max_insert_threads > 1.
for x in 1 2 3 4 5 6; do
    $CLICKHOUSE_CLIENT $SETTINGS --insert_quorum=2 --insert_quorum_parallel=0 --insert_keeper_fault_injection_probability=0 -q \
        "INSERT INTO quorum_single_stream_1 VALUES ($x)"
done

$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x), min(x), max(x) FROM quorum_single_stream_1"
$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x), min(x), max(x) FROM quorum_single_stream_2"

$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_single_stream_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_single_stream_2"
