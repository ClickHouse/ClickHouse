#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The scenarios pin the deduplication path exactly (the async batch trigger, the insert/thread
# counts), so settings randomization is disabled, as in 04613_deduplication_alias_hop_row_drift.
# Companion of 04613_deduplication_alias_hop_row_drift with a PARTITIONED deduplicating target
# behind the alias hop. Partitioning adds a path the unpartitioned test cannot reach: the sink
# splits the block by partition and DeduplicationInfo::filterToPartition attributes each token's
# source-row range to the partitions via the scatter selector. After mv1's row-count-changing
# GROUP BY re-anchored the info to the view-output chunks, the selector describes the smaller
# view-output block while the token ranges still describe the source rows, so the walk read out of
# the selector's bounds. filterToPartition must refuse such an insert with NOT_IMPLEMENTED.
# See https://github.com/ClickHouse/ClickHouse/issues/111100

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1 --parallel_view_processing=1 --max_threads=1 --max_insert_threads=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS part_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS part_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS part_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS part_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS part_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS part_dst"

# Only dst, behind the alias hop, deduplicates - and it is partitioned, so the sink splits every
# insert by partition before deduplicating.
$CLICKHOUSE_CLIENT -q "CREATE TABLE part_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE part_inner (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE part_dst (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100000"
$CLICKHOUSE_CLIENT -q "CREATE TABLE part_alias ENGINE = Alias('part_inner')"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW part_mv1 TO part_alias AS SELECT x FROM part_src GROUP BY x"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW part_mv2 TO part_dst AS SELECT x FROM part_inner"

# A single data-fed insert carries one deduplication token, so the partition split keeps the whole
# info for every partition and only the cached data hash is used: the repeated insert must
# deduplicate in both partitions of dst.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO part_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM part_src), (SELECT count() FROM part_inner), (SELECT count() FROM part_dst), (SELECT count(DISTINCT _partition_id) FROM part_dst)"

for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO part_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM part_src), (SELECT count() FROM part_inner), (SELECT count() FROM part_dst), (SELECT count(DISTINCT _partition_id) FROM part_dst)"

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE part_src"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE part_inner"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE part_dst"

# The same with async inserts: each flush carries one token, so the partition split stays on the
# single-token fast path and the repeated flush is deduplicated as a whole in both partitions.
ASYNC_SETTINGS="$SETTINGS --async_insert=1 --wait_for_async_insert=1 --async_insert_deduplicate=1"

for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $ASYNC_SETTINGS -q "INSERT INTO part_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM part_src), (SELECT count() FROM part_inner), (SELECT count() FROM part_dst), (SELECT count(DISTINCT _partition_id) FROM part_dst)"

for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $ASYNC_SETTINGS -q "INSERT INTO part_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM part_src), (SELECT count() FROM part_inner), (SELECT count() FROM part_dst), (SELECT count(DISTINCT _partition_id) FROM part_dst)"

# Two async inserts batched into one flush carry two deduplication tokens, so the partitioned sink
# must attribute each token's source rows to the partitions - impossible after mv1's GROUP BY
# collapsed the batch, so the flush is rejected with NOT_IMPLEMENTED by filterToPartition (pre-fix
# this walked the source-row ranges over the smaller partition selector: an out-of-bounds read).
# Both waiting inserts report the flush error. The batch is joined by a count trigger
# (async_insert_max_query_number=2, the has_enough_queries path); concurrently running tests
# execute SYSTEM FLUSH ASYNC INSERT QUEUE, which can flush the first insert alone before the
# second is queued, so a split batch (the outputs are not both 1: each flush then carries a single
# token and succeeds) is retried, as in 04613_deduplication_alias_hop_row_drift. A regression
# fails every attempt. The busy timeout is a bounded fallback so a split attempt's stranded insert
# flushes promptly instead of hanging the test.
# grep -m1 -c prints exactly one count per insert: the server also echoes the exception through
# send_logs_level, so the raw number of matching lines is not stable.
BATCH_SETTINGS="$ASYNC_SETTINGS --async_insert_max_query_number=2 --async_insert_busy_timeout_min_ms=5000 --async_insert_busy_timeout_max_ms=5000"
BATCH_OUT_1="${CLICKHOUSE_TMP}/04621_batch_1.out"
BATCH_OUT_2="${CLICKHOUSE_TMP}/04621_batch_2.out"
for _ in $(seq 1 10)
do
    for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $BATCH_SETTINGS -q "INSERT INTO part_src FORMAT TSV" 2>&1 | grep -m1 -c "NOT_IMPLEMENTED" > "$BATCH_OUT_1" &
    for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $BATCH_SETTINGS -q "INSERT INTO part_src FORMAT TSV" 2>&1 | grep -m1 -c "NOT_IMPLEMENTED" > "$BATCH_OUT_2" &
    wait
    if [ "$(cat "$BATCH_OUT_1")" = "1" ] && [ "$(cat "$BATCH_OUT_2")" = "1" ]
    then
        break
    fi
done
cat "$BATCH_OUT_1" "$BATCH_OUT_2"

$CLICKHOUSE_CLIENT -q "DROP TABLE part_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_src"
