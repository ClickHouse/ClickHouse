#!/usr/bin/env bash

# A materialized-view branch pruned from the write graph - its dropped target table is ignored by
# `ignore_materialized_views_with_dropped_target_table` - never creates a sink, so it must not count
# as a potential quorum writer: a non-parallel quorum `INSERT SELECT` whose live graph only reaches a
# plain `MergeTree` table keeps its `max_insert_threads` fan-out.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0"
QUORUM_SETTINGS="$SETTINGS --insert_quorum=2 --insert_quorum_parallel=0 --ignore_materialized_views_with_dropped_target_table=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_broken_view_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_broken_view_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_broken_view_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS quorum_broken_view_target"

$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_broken_view_dst (x UInt32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_broken_view_alias ENGINE = Alias('quorum_broken_view_dst')"
$CLICKHOUSE_CLIENT -q "CREATE TABLE quorum_broken_view_target (x UInt32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW quorum_broken_view_mv TO quorum_broken_view_target AS SELECT x FROM quorum_broken_view_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_broken_view_target"

$CLICKHOUSE_CLIENT $QUORUM_SETTINGS -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_broken_view_dst SELECT number FROM numbers(4)" | grep -c "MergeTreeSink"

# The same pruned graph remains pruned through an Alias. The nested INSERT expands this graph at
# runtime, so the Alias probes must apply the same pruning rules as the visible graph; otherwise
# they falsely find the catalog entry and collapse the four AliasSinks to one.
$CLICKHOUSE_CLIENT $QUORUM_SETTINGS -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_broken_view_alias SELECT number FROM numbers(4)" | grep -c "AliasSink"
$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=0 --ignore_materialized_views_with_dropped_target_table=1 -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_broken_view_alias SELECT number FROM numbers(4)" | grep -c "AliasSink"

# `materialized_views_ignore_errors` prunes the same missing-target branch. The hidden Alias probes
# must not serialize the outer fan-out when the nested INSERT will ignore that view.
$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=0 --materialized_views_ignore_errors=1 -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_broken_view_alias SELECT number FROM numbers(4)" | grep -c "AliasSink"
$CLICKHOUSE_CLIENT $SETTINGS --insert_quorum=2 --insert_quorum_parallel=0 --materialized_views_ignore_errors=1 -q \
    "EXPLAIN PIPELINE INSERT INTO quorum_broken_view_alias SELECT number FROM numbers(4)" | grep -c "AliasSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_broken_view_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_broken_view_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE quorum_broken_view_dst"
