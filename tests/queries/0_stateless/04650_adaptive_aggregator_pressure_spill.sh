#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A pressure sweep detaches and spills a drain table only once it holds at least a million keys
# (`adaptive_pressure_spill_min_keys`), so the staged stream must carry enough distinct keys to
# cross that floor - the external cells of 04649 stay far below it and only cover the drain
# itself. Here nearly every one of the ~2.6M rows misses the tiny frozen tables and is staged,
# so the drain crosses the floor more than twice and still leaves a sub-floor residue for the
# finish path: one query exercises repeated detach-and-spill cycles, the shared-table tail
# regime, and the finish drain of the remainder. The cell compares the same query with the
# feature off (and no forced spilling) and on; `AdaptiveAggregationPressureDrainedRecords`
# proves the volume, and `ExternalAggregationWritePart` proves the spill really wrote parts.
# The test runs in one `clickhouse-local` process, so the counters belong to this test alone.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET adaptive_aggregator_freeze_threshold = 128;
SET group_by_two_level_threshold = 100000000;
SET group_by_two_level_threshold_bytes = 5000000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT 'Routing table spills once it crosses the key floor';
SELECT
    (SELECT count(), sum(c) FROM (SELECT number % 1300000 AS g, count() AS c FROM numbers_mt(2600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c) FROM (SELECT number % 1300000 AS g, count() AS c FROM numbers_mt(2600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1));

SELECT 'The drain processed multiple spill floors of records';
SELECT coalesce(sum(value), 0) >= 2000000 FROM system.events WHERE event = 'AdaptiveAggregationPressureDrainedRecords';

SELECT 'The spill was exercised';
SELECT coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'ExternalAggregationWritePart';
"
