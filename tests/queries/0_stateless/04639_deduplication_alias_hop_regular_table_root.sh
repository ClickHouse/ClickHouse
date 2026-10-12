#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# These scenarios pin the deduplication path exactly: the squash thresholds decide how many
# deduplication tokens an insert carries behind the alias hop, and a randomized deduplication
# window on the destination table would change the outcome.
# Regression test: unlike 04638, the insert goes into a REGULAR table, so the outer insert
# chain's root view is empty. Every InsertDependenciesBuilder stores the empty root in its
# `inner_tables`, so an ownership check on the retry's anchor alone accepts a foreign empty
# anchor - and the retry loop then silently skipped the outer chain's intermediate views
# (dropping their transformations), after which the rebuilt rows no longer matched the
# surviving tokens and were silently LOST. The retry path must be rejected with a clean
# NOT_IMPLEMENTED whenever any of its elements is not owned by the builder rebuilding it.
# See https://github.com/ClickHouse/ClickHouse/issues/111100

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1 --parallel_view_processing=1 --max_threads=1 --max_insert_threads=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_inner"

# The insert goes into the regular table hop_src (empty root view in the outer chain). The rows
# flow through hop_mv1 into the alias, and behind the hop through hop_mv2 into the
# deduplicating hop_dst.
$CLICKHOUSE_CLIENT -q "CREATE TABLE hop_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE hop_inner (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE hop_dst (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100000"
$CLICKHOUSE_CLIENT -q "CREATE TABLE hop_alias ENGINE = Alias('hop_inner')"
# hop_mv1 is row-preserving but NOT an identity: a retry chain that skipped it (because its
# builder does not own the view) would push unshifted rows to the destination - visible data
# corruption instead of a crash.
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW hop_mv1 TO hop_alias AS SELECT x + 1000000 AS x FROM hop_src"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW hop_mv2 TO hop_dst AS SELECT x FROM hop_inner"

# A single-token data-fed insert (deduplication is not active for INSERT SELECT).
seq 1 400 | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO hop_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM hop_inner), (SELECT count() FROM hop_dst)"

# The same insert again: the whole block collides at the destination. Nothing is left to retry,
# so the retry chain must not be built; the repeated block is deduplicated cleanly.
seq 1 400 | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO hop_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM hop_inner), (SELECT count() FROM hop_dst)"

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE hop_src"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE hop_inner"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE hop_dst"

# A partial collision that DOES need the retry chain. The source-side squashing
# (min_insert_block_size_rows) re-blocks the 800 input rows into two 400-row source blocks - two
# tokens - and min_insert_block_size_rows_for_materialized_views makes the nested squash behind
# the alias hop merge them into one block, so the destination sees one block with two tokens. The
# first source block repeats next to a fresh one: exactly one of the two tokens collides, and the
# retry path contains hop_mv1, which the nested chain's builder does not own. The insert is
# rejected with a clean NOT_IMPLEMENTED instead of silently losing the surviving rows.
SPLIT_SETTINGS="$SETTINGS --async_insert=0 --max_insert_block_size=400 --min_insert_block_size_rows=400 --min_insert_block_size_bytes=0 --min_insert_block_size_rows_for_materialized_views=1000000"

{ seq 1 400; seq 401 800; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO hop_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM hop_inner), (SELECT count() FROM hop_dst)"

# grep -m1 -c prints exactly one count: the server also echoes the exception through
# send_logs_level, so the raw number of matching lines is not stable.
{ seq 1 400; seq 801 1200; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO hop_src FORMAT TSV" 2>&1 | grep -m1 -c "NOT_IMPLEMENTED"
# The second count is the number of rows that skipped hop_mv1's shift - rows a wrongly-built
# retry chain would have pushed to the destination; the first would show silently LOST rows.
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM hop_dst), (SELECT count() FROM hop_dst WHERE x <= 1000000)"

$CLICKHOUSE_CLIENT -q "DROP TABLE hop_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE hop_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE hop_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE hop_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE hop_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE hop_src"
