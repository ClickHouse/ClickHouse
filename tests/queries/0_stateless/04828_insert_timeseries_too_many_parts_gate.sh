#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
# Tag no-replicated-database: Fails due to additional replicas or shards
# Tag no-shared-merge-tree: No quorum
# Tag no-async-insert: async inserts are not supported with non-parallel quorum inserts

# A TimeSeries table forwards every write through nested INSERTs into its target tables
# (TimeSeriesSink), so two sibling branches of one query converging on the same TimeSeries table
# open concurrent inner sinks of the same target table. Those nested INSERTs must share the outer
# query's insert start gates: the `Too many parts` check of a later branch must not count the parts
# an earlier branch of the same query has already committed on the data table. For the same reason
# a TimeSeries table hides its write targets from the quorum stream probes, so a non-parallel
# quorum insert through views converging on a TimeSeries table pushes them sequentially instead of
# racing two in-flight quorum parts of the inner replicated data table against each other.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--max_block_size=1 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=1 --max_insert_threads=1 --async_insert=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_gate_mv_a"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_gate_mv_b"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_gate"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_gate_source"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_gate_data"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_gate_tags"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_gate_metrics"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ts_gate_data (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS parts_to_throw_insert = 1"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ts_gate_tags (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3)) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ts_gate_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "CREATE TABLE ts_gate ENGINE = TimeSeries DATA ts_gate_data TAGS ts_gate_tags METRICS ts_gate_metrics"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ts_gate_source (x UInt64) ENGINE = MergeTree ORDER BY x"

# Two materialized views converging on the same TimeSeries table: their sinks open nested INSERTs
# into the same data table, and those nested INSERTs share the outer query's insert start gates, so
# the `Too many parts` check of the data table runs once for the whole query. Today TimeSeriesSink
# creates its nested pipelines eagerly, before anything is written - the shared gate keeps the
# checks ordered before the writes even if that ever changes (the second view passes only the last
# of the three single-row blocks, so a check running at its first write would count the parts the
# first view's branch has already committed and reject the query on `parts_to_throw_insert = 1`).
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW ts_gate_mv_a TO ts_gate AS SELECT 'metric_a' AS metric_name, map('x', toString(x)) AS tags, [(toDateTime64(x, 3), toFloat64(x))] AS time_series FROM ts_gate_source"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW ts_gate_mv_b TO ts_gate AS SELECT 'metric_b' AS metric_name, map('x', toString(x)) AS tags, [(toDateTime64(x, 3), toFloat64(x))] AS time_series FROM ts_gate_source WHERE x >= 2"

$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=0 -q "INSERT INTO ts_gate_source SELECT number FROM numbers(3)"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM ts_gate_data"

$CLICKHOUSE_CLIENT -q "DROP TABLE ts_gate_mv_a"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_gate_mv_b"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_gate"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_gate_source"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_gate_data"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_gate_tags"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_gate_metrics"

# Non-parallel quorum inserts through two views converging on one TimeSeries table whose data
# table is replicated: the TimeSeries table hides whether the write reaches a ReplicatedMergeTree
# table, so the probes fail closed and the view branches are pushed sequentially - each branch's
# quorum is satisfied before the next branch commits its part.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_quorum_mv_a"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_quorum_mv_b"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_quorum"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_quorum_source"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_quorum_data_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_quorum_data_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_quorum_tags"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ts_quorum_metrics"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ts_quorum_data_1 (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04828/ts_quorum_data', '1') ORDER BY (id, timestamp)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ts_quorum_data_2 (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04828/ts_quorum_data', '2') ORDER BY (id, timestamp)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ts_quorum_tags (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3)) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ts_quorum_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "CREATE TABLE ts_quorum ENGINE = TimeSeries DATA ts_quorum_data_1 TAGS ts_quorum_tags METRICS ts_quorum_metrics"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ts_quorum_source (x UInt64) ENGINE = Null"

$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW ts_quorum_mv_a TO ts_quorum AS SELECT 'metric_a' AS metric_name, map('x', toString(x)) AS tags, [(toDateTime64(x, 3), toFloat64(x))] AS time_series FROM ts_quorum_source"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW ts_quorum_mv_b TO ts_quorum AS SELECT 'metric_b' AS metric_name, map('x', toString(x)) AS tags, [(toDateTime64(x + 100, 3), toFloat64(x))] AS time_series FROM ts_quorum_source"

for x in 1 2 3; do
    $CLICKHOUSE_CLIENT $SETTINGS --insert_quorum=2 --insert_quorum_parallel=0 --parallel_view_processing=1 --insert_keeper_fault_injection_probability=0 -q \
        "INSERT INTO ts_quorum_source VALUES ($x)"
done

$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count() FROM ts_quorum_data_1"
$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count() FROM ts_quorum_data_2"

$CLICKHOUSE_CLIENT -q "DROP TABLE ts_quorum_mv_a"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_quorum_mv_b"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_quorum"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_quorum_source"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_quorum_data_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_quorum_data_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_quorum_tags"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_quorum_metrics"
