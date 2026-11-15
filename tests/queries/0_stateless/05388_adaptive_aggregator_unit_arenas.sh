#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive merge creates the states of every merge unit in a fresh arena, which goes once the unit is converted,
# unless the unit's chunk captured it with states that point into it. Most buckets here take two units: nearly every
# row misses the small frozen tables and is staged, so a bucket holds more than twice the records of a unit, and four
# threads give two partitions per bucket. The shapes cover the ways a chunk relates to the arena of its unit: final
# results copied out of it, from string and array states that keep their data in the arena; -State results, whose
# columns capture it; and the non-final chunks of an external merge, which starts because the frozen tables of growing
# states spill. The staged records of that shape spill as well, so its units stream them back a block at a time and
# copy the string keys they keep into their arenas. Every shape compares its result with the same query with the
# feature off, and prints whether the merge took more units than buckets, whether staged records spilled and whether
# the merge went external. The thaw is off, so the rows stay staged, and the thresholds are pinned because the runner
# randomizes them. Each shape runs in its own clickhouse-local process, so the counters in `system.events` belong to
# it alone.
function run_shape()
{
    local query=$1
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1${2:+, $2}}
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

    SELECT 'matches the baseline', (${off}) = (${on});
    SELECT 'more units than buckets, staged records spilled, merge went external',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationMergeUnits') > 256,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpills') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationMerge') > 0;
    "
}

echo 'Final string and array results'
run_shape "SELECT count(), sum(cityHash64(k, s, arraySort(a))) FROM (
    SELECT number % 4500000 AS k, any(repeat('s', number % 100)) AS s, groupArray(number) AS a FROM numbers_mt(9000000)
    GROUP BY k SETTINGS __SETTINGS__)"

echo '-State results'
run_shape "SELECT count(), sum(cityHash64(k, arraySort(finalizeAggregation(st)))) FROM (
    SELECT number % 4500000 AS k, groupArrayState(number) AS st FROM numbers_mt(9000000) GROUP BY k SETTINGS __SETTINGS__)"

echo 'Non-final chunks of an external merge'
run_shape "SELECT count(), sum(cityHash64(g, arraySort(a))) FROM (
    SELECT if(number % 3 = 0, toString(number % 500), concat(toString(number % 4500000), repeat('c', number % 30))) AS g,
        groupArray(number) AS a
    FROM numbers_mt(13000000) GROUP BY g SETTINGS __SETTINGS__)" "max_bytes_before_external_group_by = 104857600"
