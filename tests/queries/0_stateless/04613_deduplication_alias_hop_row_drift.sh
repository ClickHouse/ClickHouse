#!/usr/bin/env bash
# Regression test: a dependent materialized view with a row-count-changing inner query (GROUP BY)
# targeting an Alias, with a deduplicating table behind the alias hop. The AliasSink runs a nested
# INSERT whose squashing and AddDeduplicationInfoTransform re-anchor the DeduplicationInfo's
# original block to the view-output chunks, which no longer match the source rows its offsets
# describe. Computing the deduplication data hash after that re-anchoring read out of the block's
# bounds: an abort on 'block.rows() == getRows()' in debug/sanitizer builds, a garbage hash (broken
# deduplication behind the alias) in release builds. The hashes must be cached at the alias hop,
# while the info is still consistent, so repeated identical inserts deduplicate deterministically.
# See https://github.com/ClickHouse/ClickHouse/issues/111100

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1 --parallel_view_processing=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS drift_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS drift_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS drift_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS drift_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS drift_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS drift_dst"

# Only dst, behind the alias hop, deduplicates. mv1's GROUP BY makes the view output 100 rows from
# the 400-row source block, so the nested INSERT the AliasSink runs sees chunks whose row count
# differs from the rows the restored deduplication info describes.
$CLICKHOUSE_CLIENT -q "CREATE TABLE drift_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE drift_inner (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE drift_dst (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100000"
$CLICKHOUSE_CLIENT -q "CREATE TABLE drift_alias ENGINE = Alias('drift_inner')"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW drift_mv1 TO drift_alias AS SELECT x FROM drift_src GROUP BY x"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW drift_mv2 TO drift_dst AS SELECT x FROM drift_inner"

# A data-fed insert (deduplication is not active for INSERT SELECT). 400 rows, 100 distinct.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM drift_src), (SELECT count() FROM drift_inner), (SELECT count() FROM drift_dst)"

# The same insert again: src and inner do not deduplicate and double, while dst must deduplicate
# the repeated block - its deduplication hash is computed from the consistent source block, not
# from whatever the drifted original block points at.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM drift_src), (SELECT count() FROM drift_inner), (SELECT count() FROM drift_dst)"

$CLICKHOUSE_CLIENT -q "DROP TABLE drift_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_src"
