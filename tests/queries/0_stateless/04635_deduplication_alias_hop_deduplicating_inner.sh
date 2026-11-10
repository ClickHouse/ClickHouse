#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# These scenarios pin the deduplication path exactly: the squash thresholds and the insert/thread
# counts decide how many deduplication tokens an insert carries behind the alias hop, and a
# randomized deduplication window on the inner table would change the outcome.
# Regression test: the table behind the `Alias` engine deduplicates ITSELF (it is the direct
# target of the nested INSERT the AliasSink runs, not a table behind another materialized view).
# The deduplication info restored onto the nested chain keeps the visited views of the OUTER
# insert chain, which the nested chain's InsertDependenciesBuilder does not know about. When a
# repeated insert collided at the inner table, the deduplication retry looked those views up in
# the nested builder's maps: a bare `std::out_of_range` from `unordered_map::at` in
# `InsertDependenciesBuilder::createRetry`, reported as a logical error (an abort in sanitizer
# builds). Fixed twice over: a fully-collided (empty) retry does not build the retry chain at all,
# and a partial retry that would need the outer chain's views is rejected with a clean
# NOT_IMPLEMENTED.
# See https://github.com/ClickHouse/ClickHouse/issues/111100

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1 --parallel_view_processing=1 --max_threads=1 --max_insert_threads=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hop_inner"

# The inner table behind the alias deduplicates itself. mv1's GROUP BY makes the view output 100
# rows from the 400-row source block, so the deduplication info behind the hop is re-anchored to
# a block that no longer matches the rows its offsets describe.
$CLICKHOUSE_CLIENT -q "CREATE TABLE hop_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE hop_inner (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100000"
$CLICKHOUSE_CLIENT -q "CREATE TABLE hop_alias ENGINE = Alias('hop_inner')"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW hop_mv1 TO hop_alias AS SELECT x FROM hop_src GROUP BY x"

# A data-fed insert (deduplication is not active for INSERT SELECT). 400 rows, 100 distinct,
# a single deduplication token.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO hop_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM hop_src), (SELECT count() FROM hop_inner)"

# The same insert again: the whole block collides at the inner table. All tokens are filtered,
# nothing is left to retry, so the retry chain - which the nested chain's builder could not even
# construct - must not be built; the repeated block is deduplicated cleanly.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO hop_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM hop_src), (SELECT count() FROM hop_inner)"

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE hop_src"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE hop_inner"

# A partial collision that DOES need the retry chain: mv1 is replaced with a row-preserving view,
# so the deduplication info is not drifted and slicing the collided token's rows out of the block
# succeeds, but recalculating the view output for the surviving rows would need the outer chain's
# views, which the nested chain's builder does not know. The insert is rejected with a clean
# NOT_IMPLEMENTED instead of a bare std::out_of_range. The source-side squashing
# (min_insert_block_size_rows) re-blocks the 800 input rows into two 400-row source blocks - two
# tokens - and min_insert_block_size_rows_for_materialized_views makes the nested squash merge
# them into one block behind the hop.
$CLICKHOUSE_CLIENT -q "DROP TABLE hop_mv1"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW hop_mv1 TO hop_alias AS SELECT x FROM hop_src"

SPLIT_SETTINGS="$SETTINGS --async_insert=0 --max_insert_block_size=400 --min_insert_block_size_rows=400 --min_insert_block_size_bytes=0 --min_insert_block_size_rows_for_materialized_views=1000000"

{ for _ in $(seq 1 4); do seq 1 100; done; for _ in $(seq 1 4); do seq 101 200; done; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO hop_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM hop_src), (SELECT count() FROM hop_inner)"

# The first source block repeats next to a fresh one: exactly one of the two tokens collides.
# grep -m1 -c prints exactly one count: the server also echoes the exception through
# send_logs_level, so the raw number of matching lines is not stable.
{ for _ in $(seq 1 4); do seq 1 100; done; for _ in $(seq 1 4); do seq 201 300; done; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO hop_src FORMAT TSV" 2>&1 | grep -m1 -c "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM hop_inner"

$CLICKHOUSE_CLIENT -q "DROP TABLE hop_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE hop_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE hop_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE hop_src"
