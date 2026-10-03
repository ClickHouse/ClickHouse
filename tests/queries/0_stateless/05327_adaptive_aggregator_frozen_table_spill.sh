#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator admits no new keys, so it grows only through states that keep
# growing after the freeze: in the arena (`groupArray`) or in heap memory of their own (`uniqExact`). Over
# the external-aggregation threshold such a table is written to disk as a part of the ordinary external
# aggregation, its producer learns the keys again from empty and freezes anew, and the merge goes external,
# reading the parts next to the staged records. A table whose states do not grow (`sum`, `count`) is never
# written; only its staged records are. The tables freeze at the two-level condition, a megabyte of tracked
# memory, when few groups hold them, and at a thousand keys otherwise. Every shape compares its result with
# the same query with the feature off and no forced spilling; a threshold of one byte keeps the query over
# it on every block. The thresholds are pinned because the runner randomizes them, and each shape runs in
# its own clickhouse-local process, so the counters in `system.events` belong to it alone.
function run_shape()
{
    local query=$1
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1}
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4;
    SET max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 1000;
    SET adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000;
    SET group_by_two_level_threshold_bytes = 1000000;
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 0;
    SET max_bytes_ratio_before_external_group_by = 0;
    SET query_plan_aggregation_bucket_top_k = 1;

    SELECT 'matches the baseline', (${off}) = (${on});
    SELECT 'frozen tables written to disk',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationFrozenTableSpills') > 0;
    SELECT 'staged records written to disk',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords') > 0;
    "
}

echo 'Few groups of heap states'
run_shape "SELECT count(), sum(u) FROM (
    SELECT toUInt64(number % 50) AS g, uniqExact(number) AS u FROM numbers_mt(1000000) GROUP BY g SETTINGS __SETTINGS__)"

echo 'Few groups of arena states'
run_shape "SELECT count(), sum(length(a)), sum(arraySum(a)) FROM (
    SELECT toUInt64(number % 50) AS g, groupArray(number) AS a FROM numbers_mt(1000000) GROUP BY g SETTINGS __SETTINGS__)"

# Half of the rows repeat a few hundred hot keys, which the frozen tables hold and whose arrays grow; the
# other half are distinct cold keys, which are staged, so the tables and the staged records both spill.
echo 'Hot keys of arena states next to staged cold keys'
run_shape "SELECT count(), sum(length(a)), sum(arraySum(a)) FROM (
    SELECT if(number % 2 = 0, number % 500, number) AS g, groupArray(number) AS a FROM numbers_mt(1000000)
    GROUP BY g SETTINGS __SETTINGS__)"

echo 'States that do not grow keep the tables in memory'
run_shape "SELECT count(), sum(s), sum(c) FROM (
    SELECT if(number % 2 = 0, number % 500, number) AS g, sum(number) AS s, count() AS c FROM numbers_mt(1000000)
    GROUP BY g SETTINGS __SETTINGS__)"

# The rank of `ORDER BY ... DESC LIMIT` arms the top-K pruning of the adaptive merge, which the external
# merge gives up. Group `g` holds about 100000 / (g + 1) distinct values, so the five largest are distinct.
echo 'Top-K by a distinct count of heap states'
run_shape "SELECT arraySort(groupArray((g, u))) FROM (
    SELECT toUInt64(number % 16) AS g, uniqExact(intDiv(number, 16 * (g + 1))) AS u FROM numbers_mt(1600000)
    GROUP BY g ORDER BY u DESC LIMIT 5 SETTINGS __SETTINGS__)"
