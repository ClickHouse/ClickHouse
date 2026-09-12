#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The scenarios pin the deduplication path exactly (the squash thresholds, the insert/thread
# counts), so settings randomization is disabled, as in 04621_deduplication_alias_hop_partitioned_target.
#
# The production crash from a plain materialized view - NO `Alias` engine anywhere. A row-count
# changing inner query (`GROUP BY`) feeds a PARTITIONED deduplicating target. The target sink splits
# the view-output block by partition and DeduplicationInfo::filterToPartition attributes each token's
# source-row range to the partitions via the scatter selector. But the view changed the row count, so
# the tokens describe the source rows while the selector describes the smaller view-output block:
# there is no source-row -> partition mapping. filterToPartition must keep every token in every
# partition (the target may still deduplicate a repeated token) instead of reading out of the
# selector's bounds.
# See https://github.com/ClickHouse/clickhouse-core-incidents/issues/2006

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1 --parallel_view_processing=1 --max_threads=1 --max_insert_threads=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_drift_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_drift_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_drift_src"

# Only dst deduplicates, and it is partitioned, so the sink splits every insert by partition before
# deduplicating. The view's GROUP BY reduces the row count between src and dst.
$CLICKHOUSE_CLIENT -q "CREATE TABLE mv_drift_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mv_drift_dst (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100000"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW mv_drift_mv TO mv_drift_dst AS SELECT x FROM mv_drift_src GROUP BY x"

# A single data-fed insert carries one deduplication token, so the partition split keeps the whole
# info for every partition (single-token fast path) and only the cached data hash is used: the
# repeated insert must deduplicate in both partitions of dst.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO mv_drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_drift_src), (SELECT count() FROM mv_drift_dst), (SELECT count(DISTINCT _partition_id) FROM mv_drift_dst)"

for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS -q "INSERT INTO mv_drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_drift_src), (SELECT count() FROM mv_drift_dst), (SELECT count(DISTINCT _partition_id) FROM mv_drift_dst)"

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE mv_drift_src"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE mv_drift_dst"

# Two deduplication tokens in one sync insert: the source-side squashing (min_insert_block_size_rows,
# with max_insert_block_size as the parser cap) re-blocks the 800 input rows into two 400-row source
# blocks, each carrying its own token, and the view-input squashing concatenates them into one block
# before the row-count-changing GROUP BY. The partitioned sink then sees two tokens over a block that
# no longer matches the tokens' source rows. filterToPartition keeps both tokens in both partitions
# (pre-fix it walked the source-row ranges over the smaller partition selector: an out-of-bounds
# read), so the insert fills dst and the repeated insert is deduplicated.
SPLIT_SETTINGS="$SETTINGS --async_insert=0 --max_insert_block_size=400 --min_insert_block_size_rows=400 --min_insert_block_size_bytes=0 --min_insert_block_size_rows_for_materialized_views=1000000"

{ for _ in $(seq 1 4); do seq 1 100; done; for _ in $(seq 1 4); do seq 101 200; done; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO mv_drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_drift_dst), (SELECT count(DISTINCT _partition_id) FROM mv_drift_dst)"

{ for _ in $(seq 1 4); do seq 1 100; done; for _ in $(seq 1 4); do seq 101 200; done; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO mv_drift_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_drift_dst), (SELECT count(DISTINCT _partition_id) FROM mv_drift_dst)"

$CLICKHOUSE_CLIENT -q "DROP TABLE mv_drift_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE mv_drift_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE mv_drift_src"
