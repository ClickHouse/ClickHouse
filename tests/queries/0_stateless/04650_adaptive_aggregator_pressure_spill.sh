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

# A key freeze threshold above both the key count and the give-up bound, with the byte bound
# disabled, keeps every producer in the learning phase for the whole query. The learning phase
# is the one regime where no frozen table is ever built.
# A learning table must still spill: the counter has to advance across the query, and the query
# has to fit in a limit that only the spilling arm can meet. Both external thresholds are pinned
# per query because the runner randomizes them, and `max_memory_usage` is the cell that encodes
# the user-visible consequence rather than a counter. A separate `clickhouse-local` process keeps
# these counters clear of the cells above.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET group_by_two_level_threshold = 1000;
SET collect_hash_table_stats_during_aggregation = 0;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 4000000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET max_bytes_before_external_group_by = 20000000;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT 'Learning-phase tables spill under the external threshold';
SELECT count() FROM (SELECT concat(toString(number), repeat('x', number % 40)) AS k, count() AS c FROM numbers_mt(3000000) GROUP BY k) FORMAT Null;
SELECT coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'ExternalAggregationWritePart';

SELECT 'The learning phase stood down under pressure';
SELECT coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns';

SELECT 'A learning-phase query fits in a limit only spilling can meet';
SELECT count() FROM (SELECT concat(toString(number), repeat('x', number % 40)) AS k, count() AS c FROM numbers_mt(3000000) GROUP BY k)
SETTINGS max_memory_usage = 700000000;
"

# The same shape with no external threshold: the pressure trigger cannot fire, so the producers
# stay in the learning phase and the counter must not move.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET group_by_two_level_threshold = 1000;
SET collect_hash_table_stats_during_aggregation = 0;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 4000000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT 'No pressure, no stand-down';
SELECT count() FROM (SELECT concat(toString(number), repeat('x', number % 40)) AS k, count() AS c FROM numbers_mt(3000000) GROUP BY k) FORMAT Null;
SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns';
"
