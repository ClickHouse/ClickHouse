#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Over the external-aggregation threshold a frozen producer writes its staged records to the
# session's spill streams and frees them; the merge task of every bucket reads the bucket's stream
# back and merges it with the records still in memory. Here nearly every one of the ~2.6M rows
# misses the tiny frozen tables and is staged, and a threshold of one byte keeps the query over it
# on every block, so the producers spill again and again and the merge reads almost every record
# from disk. The cell compares the same query with the feature off (and no forced spilling) and
# on; `AdaptiveAggregationSpilledRecords` proves the volume, `ExternalAggregationCompressedBytes`
# that the spill really reached the disk. The test runs in one `clickhouse-local` process, so the
# counters belong to this test alone.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET adaptive_aggregator_freeze_threshold = 128;
SET group_by_two_level_threshold = 100000000;
SET group_by_two_level_threshold_bytes = 5000000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT 'Spilled staged records merge back exactly';
SELECT
    (SELECT count(), sum(c) FROM (SELECT number % 1300000 AS g, count() AS c FROM numbers_mt(2600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c) FROM (SELECT number % 1300000 AS g, count() AS c FROM numbers_mt(2600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1));

SELECT 'The producers spilled almost every staged record';
SELECT coalesce(sum(value), 0) >= 2000000 FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords';

SELECT 'The spill reached the disk';
SELECT coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'ExternalAggregationCompressedBytes';
"
