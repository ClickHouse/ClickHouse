#!/usr/bin/env bash
# Regression test: an Alias forwarding to a deduplicating table must not disable the
# max_insert_threads write fan-out for a plain INSERT without strict insert block limits. The
# AliasSink runs its nested INSERT in this query's context with the chunk's DeduplicationInfo intact,
# and the nested AddDeduplicationInfoTransform does not restamp a chunk that has already visited a
# view - and the single-stream head of the outer pipeline always pushes the root entry via
# setRootViewID before the fan-out. So the globally stamped source block numbering survives the Alias
# hop, identical blocks on different branches keep distinct deduplication ids, and the fan-out is as
# safe as for an INSERT into the alias target itself. This holds both for a direct
# INSERT INTO alias(deduplicating table) and for a dependent materialized view targeting such an
# alias (src -> mv TO alias(deduplicating dst)).
#
# Under use_strict_insert_block_limits the source block number is stamped per branch *after* the
# fan-out and survives the hop, so identical blocks on different branches would collide on the
# deduplicating destination: strict inserts must stay single-stream in both topologies, as must a
# strict insert into an alias whose target hides a dependent view behind the nested INSERT.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_fanout_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_fanout_dst"

$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_fanout_dst (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 10000"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_fanout_alias ENGINE = Alias('alias_fanout_dst')"

# Topology 1: a direct INSERT into an Alias of a deduplicating table. Without strict limits the
# source numbering is global and survives the hop: four AliasSinks.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_fanout_alias VALUES (1)" | grep -c "AliasSink"

# Under strict limits the numbering is per-branch and would collide on the deduplicating target: a
# single AliasSink.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_fanout_alias VALUES (1)" | grep -c "AliasSink"

# Row integrity with the fan-out active: four identical 100-row blocks must all arrive in the
# deduplicating target. If the numbering restarted per branch, identical blocks on different branches
# would collide and rows would be silently dropped. Kept intentionally small so the test stays well
# under the time limit under the s3/keeper CI configuration.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO alias_fanout_alias FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x), min(x), max(x) FROM alias_fanout_dst"

# Topology 2: a dependent materialized view targeting an Alias of a deduplicating table
# (src -> mv TO alias(dst)). The deduplication happens on the immediate MV target behind the alias
# hop, visible to the dependent-view hazard scan.
MV_SETTINGS="--parallel_view_processing=1 --insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_fanout_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_fanout_src"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE alias_fanout_dst"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_fanout_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW alias_fanout_mv TO alias_fanout_alias AS SELECT x FROM alias_fanout_src"

# Without strict limits the view-level ids fold in the globally stamped source number, so they stay
# distinct across branches even behind the alias hop: four AliasSinks.
$CLICKHOUSE_CLIENT $SETTINGS $MV_SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_fanout_src VALUES (1)" | grep -c "AliasSink"

# Under strict limits the per-branch numbering survives the hop and would collide the view-level ids
# on the deduplicating target: a single AliasSink.
$CLICKHOUSE_CLIENT $SETTINGS $MV_SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_fanout_src VALUES (1)" | grep -c "AliasSink"

# Row integrity with the fan-out active: four identical 100-row blocks must all reach the
# deduplicating table behind the MV and the alias hop.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS $MV_SETTINGS --max_insert_threads=4 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO alias_fanout_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM alias_fanout_src), (SELECT count() FROM alias_fanout_dst)"

# Boundary: a strict INSERT into an Alias of a table whose dependent-view graph hides behind the
# nested INSERT (alias -> src, src has a deduplicating dependent MV). The per-branch numbering
# survives the hop into that hidden graph, so the strict insert must stay single-stream.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_fanout_src_alias"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_fanout_src_alias ENGINE = Alias('alias_fanout_src')"
$CLICKHOUSE_CLIENT $SETTINGS $MV_SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_fanout_src_alias VALUES (1)" | grep -c "AliasSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE alias_fanout_src_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_fanout_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_fanout_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_fanout_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_fanout_dst"
