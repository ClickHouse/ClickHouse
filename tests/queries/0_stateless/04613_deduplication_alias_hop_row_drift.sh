#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# These scenarios pin the deduplication path exactly: the async batch busy timeout, the view-level
# squash thresholds and the insert/thread counts decide how the deduplication tokens merge behind
# the alias hop and whether two inserts join one async flush. Settings randomization perturbs that
# (e.g. an early async flush breaks the batched self-deduplication), so it is disabled here.
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

# max_threads/max_insert_threads are pinned: with parallel processing the interleaving of the
# view-output chunks decides how the tokens merge behind the alias hop, so the deduplication token
# identity - and with it the deduplication outcome at dst - would depend on thread scheduling.
SETTINGS="--insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1 --parallel_view_processing=1 --max_threads=1 --max_insert_threads=1"

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

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE drift_src"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE drift_inner"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE drift_dst"

# The same scenario, but the view emits its output in multiple chunks: with a small max_block_size
# the GROUP BY produces 10-row blocks, and a small min_insert_block_size_rows makes the view-level
# squashing pass each of them through. Each chunk carries a clone of the same source-level
# deduplication info, distinguished only by consecutive view-block numbers. The nested INSERT
# behind the alias hop squashes with the target-side threshold - raised back to the default by
# min_insert_block_size_rows_for_materialized_views - so it merges the stamped chunks, extending
# the view-block range of the token. The cached data hashes must survive that merge, or the hash
# is recomputed from the re-anchored (drifted) block: without the fix in
# DeduplicationInfo::TokenDefinition::doExtend the second insert is not deduplicated at dst.
# max_threads=1 keeps the chunk order, and thus the merged token identity, deterministic.
MULTICHUNK_SETTINGS="$SETTINGS --async_insert=0 --max_block_size=10 --max_threads=1 --min_insert_block_size_rows=10 --min_insert_block_size_rows_for_materialized_views=1000000"

for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $MULTICHUNK_SETTINGS -q "INSERT INTO drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM drift_src), (SELECT count() FROM drift_inner), (SELECT count() FROM drift_dst)"

for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $MULTICHUNK_SETTINGS -q "INSERT INTO drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM drift_src), (SELECT count() FROM drift_inner), (SELECT count() FROM drift_dst)"

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE drift_src"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE drift_inner"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE drift_dst"

# The same scenario with async inserts: the deduplication info is flagged as an async insert, so a
# collision at the deduplicating table behind the alias hop never takes the single-token fast path
# and used to walk the source-row offsets over the drifted view-output block. With the fix a
# repeated identical flush is deduplicated as a whole, without row-level slicing.
ASYNC_SETTINGS="$SETTINGS --async_insert=1 --wait_for_async_insert=1 --async_insert_deduplicate=1"

for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $ASYNC_SETTINGS -q "INSERT INTO drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM drift_src), (SELECT count() FROM drift_inner), (SELECT count() FROM drift_dst)"

for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $ASYNC_SETTINGS -q "INSERT INTO drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM drift_src), (SELECT count() FROM drift_inner), (SELECT count() FROM drift_dst)"

# Two identical async inserts batched into one flush carry two deduplication tokens with the same
# hash, so the deduplicating sink behind the alias hop attempts self-deduplication, which needs to
# slice one token's rows out of the block. After mv1's GROUP BY collapsed the batch there is no
# mapping from the tokens' source rows to the view-output block anymore, so the flush is rejected
# with NOT_IMPLEMENTED - pre-fix this walked the source-row offsets over the smaller view-output
# block: an abort in debug builds, an out-of-bounds write in release builds. Both waiting inserts
# report the flush error. The batch is joined by a count trigger:
# async_insert_max_query_number=2 flushes the buffer exactly when the second insert is queued (the
# has_enough_queries path, which requires deduplication to be enabled), so the two inserts join one
# flush no matter how slowly a loaded runner starts the second client. Joining the batch still
# cannot be guaranteed: concurrently running tests execute SYSTEM FLUSH ASYNC INSERT QUEUE, which
# flushes the whole queue server-wide and can flush the first insert alone before the second is
# queued. A split batch means each flush self-deduplicates nothing and succeeds (the outputs are
# not both 1), so the attempt is retried; a regression of the fix fails every attempt, because the
# joined flush then never reports NOT_IMPLEMENTED. The busy timeout is only a bounded fallback so a
# split attempt's stranded insert flushes promptly instead of hanging the test.
# grep -m1 -c prints exactly one count per insert: the server also echoes the exception through
# send_logs_level, so the raw number of matching lines is not stable.
BATCH_SETTINGS="$ASYNC_SETTINGS --async_insert_max_query_number=2 --async_insert_busy_timeout_min_ms=5000 --async_insert_busy_timeout_max_ms=5000"
BATCH_OUT_1="${CLICKHOUSE_TMP}/04613_batch_1.out"
BATCH_OUT_2="${CLICKHOUSE_TMP}/04613_batch_2.out"
for _ in $(seq 1 10)
do
    for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $BATCH_SETTINGS -q "INSERT INTO drift_src FORMAT TSV" 2>&1 | grep -m1 -c "NOT_IMPLEMENTED" > "$BATCH_OUT_1" &
    for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $BATCH_SETTINGS -q "INSERT INTO drift_src FORMAT TSV" 2>&1 | grep -m1 -c "NOT_IMPLEMENTED" > "$BATCH_OUT_2" &
    wait
    if [ "$(cat "$BATCH_OUT_1")" = "1" ] && [ "$(cat "$BATCH_OUT_2")" = "1" ]
    then
        break
    fi
done
cat "$BATCH_OUT_1" "$BATCH_OUT_2"

$CLICKHOUSE_CLIENT -q "DROP TABLE drift_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE drift_src"
