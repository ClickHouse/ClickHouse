#!/usr/bin/env bash
# Regression test: `parallel_view_processing = 0` must keep the pushing to dependent materialized
# views sequential even when the view graph hides behind an Alias hop. For a visible view graph
# InsertDependenciesBuilder keeps the sink stream size at 1, but an INSERT INTO an Alias of a table
# with dependent views expands that graph only inside the nested INSERT each AliasSink runs, so a
# max_insert_threads fan-out to several AliasSinks would push the hidden views concurrently. The
# outer insert must stay single-stream in that case, independently of any deduplication hazard, and
# fan out again once parallel_view_processing is enabled or when the alias target has no dependent
# views.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_hv_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_hv_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_hv_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_hv_dst"

# No table in this topology deduplicates, so only the parallel_view_processing gate is exercised.
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_hv_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_hv_dst (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_hv_alias ENGINE = Alias('alias_hv_src')"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW alias_hv_mv TO alias_hv_dst AS SELECT x FROM alias_hv_src"

# The alias target has a dependent materialized view hidden behind the AliasSink's nested INSERT:
# with parallel_view_processing disabled the view pushing must stay sequential, so the outer INSERT
# keeps a single AliasSink despite max_insert_threads.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --parallel_view_processing=0 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_hv_alias VALUES (1)" | grep -c "AliasSink"

# With parallel_view_processing enabled the concurrent pushing of the hidden views is requested
# explicitly: four AliasSinks.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --parallel_view_processing=1 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_hv_alias VALUES (1)" | grep -c "AliasSink"

# Row integrity for the serial case: all rows must reach both the alias target and the view target.
seq 1 400 | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --parallel_view_processing=0 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO alias_hv_alias FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM alias_hv_src), (SELECT count() FROM alias_hv_dst)"

# An Alias of a table without dependent views keeps the fan-out even with
# parallel_view_processing disabled: there is nothing to push sequentially.
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_hv_mv"
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --parallel_view_processing=0 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_hv_alias VALUES (1)" | grep -c "AliasSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE alias_hv_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_hv_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_hv_dst"
