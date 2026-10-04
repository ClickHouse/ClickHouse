#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A local table of the adaptive aggregator freezes at its own bounds, a key count and a footprint, and also
# on the condition on which the baseline converts its table to two-level: a key count, or the tracked
# memory of the query. The memory catches a few groups whose states own heap memory, which neither of the
# table's own bounds sees. Every shape puts both bounds of the table out of reach, so a freeze can come
# only from the two-level condition, and a table that reaches no bound keeps learning to the end, like a
# small baseline table. Every shape compares its result with the same query with the feature off. The
# thresholds are pinned because the runner randomizes them, and each shape runs in its own
# clickhouse-local process, so the counters in `system.events` belong to it alone.
function run_shape()
{
    local two_level_keys=$1 two_level_bytes=$2 query=$3
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1}
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4;
    SET max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 100000000;
    SET adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = ${two_level_keys};
    SET group_by_two_level_threshold_bytes = ${two_level_bytes};
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 0;
    SET max_bytes_ratio_before_external_group_by = 0;

    SELECT 'matches the baseline', (${off}) = (${on});
    SELECT 'froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
    "
}

echo 'The key count of the two-level condition'
run_shape 1000 0 "SELECT count(), sum(c) FROM (
    SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS __SETTINGS__)"

echo 'The tracked memory of the two-level condition, over few groups of heap states'
run_shape 0 1000000 "SELECT count(), sum(u) FROM (
    SELECT toUInt64(number % 50) AS g, uniqExact(number) AS u FROM numbers_mt(400000) GROUP BY g SETTINGS __SETTINGS__)"

echo 'No bound reached: the tables keep learning'
run_shape 100000 50000000 "SELECT count(), sum(s) FROM (
    SELECT toUInt64(number % 50) AS g, sum(number) AS s FROM numbers_mt(400000) GROUP BY g SETTINGS __SETTINGS__)"
