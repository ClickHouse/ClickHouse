#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lower bound of `HAVING count()` prunes the adaptive merge as a fixed threshold: the count bins of the producers bound
# the count of every group a merge unit holds, so a unit whose bins all stay below the bound is skipped and its staged
# records are freed unread. The source repeats 50 hot keys on a tenth of the rows, 4000 rows each, and spreads the rest
# over cold keys, so only the hot keys pass the bounds below, and the tables, frozen at 128 keys, stage the cold keys.
# Every shape compares its result with the same query with the feature off and reports whether units were pruned and
# whether threads thawed. An upper bound (`<`) bounds no group from above, so it prunes nothing; a top-K by `count()`
# keeps its own pruning. The thresholds are pinned because the runner randomizes them, and each shape runs in its own
# clickhouse-local process, so the counters in `system.events` belong to it alone.
function run_shape()
{
    local query=$1
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1}
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 5000000000;
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

    SELECT 'matches the baseline', (${off}) = (${on});
    SELECT 'pruned units', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedUnits') > 0;
    SELECT 'thawed', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') > 0;
    "
}

KEY="if(number % 10 = 0, number % 50, 50 + number % 200000)"

echo 'count() > bound'
run_shape "SELECT count(), sum(c), sum(s) FROM (
    SELECT $KEY AS k, count() AS c, sum(number) AS s FROM numbers_mt(2000000) GROUP BY k HAVING count() > 1000 SETTINGS __SETTINGS__)"

echo 'count() >= bound'
run_shape "SELECT count(), sum(c), sum(s) FROM (
    SELECT $KEY AS k, count() AS c, sum(number) AS s FROM numbers_mt(2000000) GROUP BY k HAVING count() >= 4000 SETTINGS __SETTINGS__)"

echo 'count() = bound'
run_shape "SELECT count(), sum(c), sum(s) FROM (
    SELECT $KEY AS k, count() AS c, sum(number) AS s FROM numbers_mt(2000000) GROUP BY k HAVING count() = 4000 SETTINGS __SETTINGS__)"

echo 'count() < bound prunes nothing'
run_shape "SELECT count(), sum(c), sum(s) FROM (
    SELECT $KEY AS k, count() AS c, sum(number) AS s FROM numbers_mt(2000000) GROUP BY k HAVING count() < 1000 SETTINGS __SETTINGS__)"

# Cold keys that a thread meets again and again, with a wide string argument: the threads thaw, and their tables count
# their rows into the bins at the finish, so the bounds still hold.
echo 'Threads that thaw'
run_shape "SELECT count(), sum(c), sum(cityHash64(m)) FROM (
    SELECT if(number % 10 = 0, number % 50, 50 + number % 20000) AS k, count() AS c, min(repeat(toString(number), 10)) AS m
    FROM numbers_mt(2000000) GROUP BY k HAVING count() > 1000 SETTINGS __SETTINGS__)"

# The hot keys tie at 4000 rows, so which five the top-K keeps is free: the shape compares their counts only.
echo 'A top-K by count() keeps its own pruning'
run_shape "SELECT arraySort(groupArray(c)) FROM (
    SELECT $KEY AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k HAVING count() > 1000 ORDER BY c DESC LIMIT 5
    SETTINGS __SETTINGS__)"
