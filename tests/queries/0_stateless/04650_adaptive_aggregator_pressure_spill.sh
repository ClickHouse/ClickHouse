#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A pressure sweep spills the routing table mid-drain only once it holds at least a million
# keys (`adaptive_pressure_spill_min_keys`), so the staged stream must carry enough distinct
# keys to cross that floor - the external cells of 04649 stay far below it and only cover the
# drain itself. The cell compares the same query with the feature off (and no forced spilling)
# and on, and the `ExternalAggregationWritePart` counter proves the spill really ran. The test
# runs in one `clickhouse-local` process, so the counter belongs to this test alone.
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

SELECT 'The spill was exercised';
SELECT coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'ExternalAggregationWritePart';
"
