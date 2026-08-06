#!/usr/bin/env bash
# Regression test: a writable regular view forwards the insert into its underlying table through a
# nested INSERT per sink (SinkToStorageView), exactly like Alias — so the target's dependent-view
# graph is expanded only inside that nested INSERT and is hidden from InsertDependenciesBuilder.
# With use_strict_insert_block_limits the source block number is stamped per branch after the
# max_insert_threads fan-out, and it survives the view hop (the chunk has already visited a view, so
# the nested INSERT preserves its deduplication info instead of restamping it). A deduplicating view
# target behind the hop (view -> inner -> mv TO deduplicating dst) then sees colliding view-level
# ids for identical blocks on different branches and silently drops rows. Such strict inserts must
# stay single-stream; without strict limits the source numbering is global and the fan-out stays
# safe, as it does when nothing deduplicating hides behind the view.
# Mirrors 04602_max_insert_threads_mv_alias_hidden_dedup.sh for Alias.
# See https://github.com/ClickHouse/ClickHouse/pull/102866

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --parallel_view_processing=1 --insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1"

create_tables()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS view_hidden_mv"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS view_hidden_view"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS view_hidden_inner"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS view_hidden_dst"
    # The view target inner does not deduplicate - only dst, the target of the materialized view
    # reading from inner, does. This isolates the hidden dependent-view hazard from every
    # deduplication path the builder can see directly.
    $CLICKHOUSE_CLIENT -q "CREATE TABLE view_hidden_inner (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE view_hidden_dst (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100000"
    $CLICKHOUSE_CLIENT -q "CREATE VIEW view_hidden_view AS SELECT x FROM view_hidden_inner"
    $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW view_hidden_mv TO view_hidden_dst AS SELECT x FROM view_hidden_inner"
}

create_tables

# A strict insert must stay single-stream: the deduplicating dst hides behind the view hop, so the
# per-branch source numbering would collide the view-level ids there.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 -q \
    "EXPLAIN PIPELINE INSERT INTO view_hidden_view VALUES (1)" | grep -c "SinkToStorageView"

# Without strict limits the source numbering is stamped globally before the fan-out and survives the
# view hop, so the view-level ids stay distinct across branches and the insert fans out.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO view_hidden_view VALUES (1)" | grep -c "SinkToStorageView"

# Row integrity for the strict insert: four identical 100-row blocks must all arrive in dst. With a
# per-branch fan-out, identical blocks on different branches would produce identical view-level ids
# behind the view hop and dst would silently lose blocks. Kept intentionally small so the test stays
# well under the time limit under the s3/keeper CI configuration.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --use_strict_insert_block_limits=1 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO view_hidden_view FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM view_hidden_inner), (SELECT count() FROM view_hidden_dst)"

# The same without strict limits: the fan-out is active and all rows still arrive.
create_tables
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO view_hidden_view FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM view_hidden_inner), (SELECT count() FROM view_hidden_dst)"

# When nothing hides behind the view (no materialized view over the target), even a strict insert
# fans out: the guard fails closed only on the presence of a dependent view behind the hop.
$CLICKHOUSE_CLIENT -q "DROP TABLE view_hidden_mv"
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 -q \
    "EXPLAIN PIPELINE INSERT INTO view_hidden_view VALUES (1)" | grep -c "SinkToStorageView"

$CLICKHOUSE_CLIENT -q "DROP TABLE view_hidden_view"
$CLICKHOUSE_CLIENT -q "DROP TABLE view_hidden_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE view_hidden_inner"
