#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The scenario pins the deduplication path exactly (the squash thresholds, the insert/thread
# counts), so settings randomization is disabled, as in 04613_deduplication_alias_hop_row_drift.
# Companion of 04621_deduplication_alias_hop_partitioned_target with the deduplication window of
# the partitioned target set to 0: the sink does not deduplicate, so the tokens are never
# registered and DeduplicationInfo::filterToPartition has nothing to attribute. The multi-token
# insert whose deduplication info drifted behind the alias hop (mv1's row-count-changing GROUP BY
# re-anchored it to the view-output chunks) must succeed instead of being rejected with
# NOT_IMPLEMENTED by the consistency check of filterToPartition.
# See https://github.com/ClickHouse/ClickHouse/issues/111100

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1 --parallel_view_processing=1 --max_threads=1 --max_insert_threads=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nodedup_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nodedup_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nodedup_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nodedup_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nodedup_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nodedup_dst"

# No table in the chain deduplicates - dst is partitioned, so the sink still splits every insert
# by partition and calls filterToPartition, but with deduplication disabled it must pass the
# tokens through untouched.
$CLICKHOUSE_CLIENT -q "CREATE TABLE nodedup_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE nodedup_inner (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE nodedup_dst (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE nodedup_alias ENGINE = Alias('nodedup_inner')"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW nodedup_mv1 TO nodedup_alias AS SELECT x FROM nodedup_src GROUP BY x"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW nodedup_mv2 TO nodedup_dst AS SELECT x FROM nodedup_inner"

# Two deduplication tokens in one sync insert, exactly as in the rejection scenario of 04621: the
# source-side squashing re-blocks the 800 input rows into two 400-row source blocks, each carrying
# its own token, and the nested INSERT behind the alias hop squashes them into one drifted block.
# Since dst does not deduplicate, the insert must succeed and fill both partitions.
SPLIT_SETTINGS="$SETTINGS --async_insert=0 --max_insert_block_size=400 --min_insert_block_size_rows=400 --min_insert_block_size_bytes=0 --min_insert_block_size_rows_for_materialized_views=1000000"

{ for _ in $(seq 1 4); do seq 1 100; done; for _ in $(seq 1 4); do seq 101 200; done; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO nodedup_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM nodedup_src), (SELECT count() FROM nodedup_inner), (SELECT count() FROM nodedup_dst), (SELECT count(DISTINCT _partition_id) FROM nodedup_dst)"

# The repeated insert is not deduplicated anywhere: every count doubles.
{ for _ in $(seq 1 4); do seq 1 100; done; for _ in $(seq 1 4); do seq 101 200; done; } | $CLICKHOUSE_CLIENT $SPLIT_SETTINGS -q "INSERT INTO nodedup_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM nodedup_src), (SELECT count() FROM nodedup_inner), (SELECT count() FROM nodedup_dst), (SELECT count(DISTINCT _partition_id) FROM nodedup_dst)"

$CLICKHOUSE_CLIENT -q "DROP TABLE nodedup_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE nodedup_mv1"
$CLICKHOUSE_CLIENT -q "DROP TABLE nodedup_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE nodedup_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE nodedup_inner"
$CLICKHOUSE_CLIENT -q "DROP TABLE nodedup_src"
