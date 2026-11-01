#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The scenarios pin the deduplication path exactly (the squash thresholds, the insert/thread
# counts), so settings randomization is disabled, as in 04613_deduplication_alias_hop_row_drift.
# Companion of 04613_deduplication_alias_hop_row_drift with a PARTITIONED deduplicating target
# behind the alias hop. Partitioning adds a path the unpartitioned test cannot reach: the sink
# splits the block by partition and DeduplicationInfo::filterToPartition attributes each token's
# source-row range to the partitions via the scatter selector. After mv1's row-count-changing
# GROUP BY re-anchored the info to the view-output chunks, the selector describes the smaller
# view-output block while the token ranges still describe the source rows, so the walk read out of
# the selector's bounds. Because the info passed through a view, filterToPartition keeps every token
# in every partition (the target may still deduplicate a repeated token) instead of the walk.
# See https://github.com/ClickHouse/ClickHouse/issues/111100
# and https://github.com/ClickHouse/clickhouse-core-incidents/issues/2006

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

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE part_src"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE part_inner"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE part_dst"

# Two deduplication tokens in one sync insert: the source-side squashing
# (min_insert_block_size_rows, with max_insert_block_size as the parser cap) re-blocks the 800
# input rows into two 400-row source blocks, each carrying its own token, and the nested INSERT
# behind the alias hop squashes them into one block. The partitioned sink cannot attribute each
# token's source rows to the partitions - impossible after mv1's GROUP BY collapsed the blocks - so
# filterToPartition keeps both tokens in both partitions (pre-fix this walked the source-row ranges
# over the smaller partition selector: an out-of-bounds read). The insert fills dst and the repeated
# insert is deduplicated as a whole.
SPLIT_SETTINGS="$SETTINGS --async_insert=0 --max_insert_block_size=400 --min_insert_block_size_rows=400 --min_insert_block_size_bytes=0 --min_insert_block_size_rows_for_materialized_views=1000000"

{ for _ in $(seq 1 4); do seq 1 100; done; for _ in $(seq 1 4); do seq 101 200; done; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO part_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM part_dst), (SELECT count(DISTINCT _partition_id) FROM part_dst)"

{ for _ in $(seq 1 4); do seq 1 100; done; for _ in $(seq 1 4); do seq 101 200; done; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO part_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM part_dst), (SELECT count(DISTINCT _partition_id) FROM part_dst)"

$CLICKHOUSE_CLIENT -q "DROP TABLE part_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE part_src"
