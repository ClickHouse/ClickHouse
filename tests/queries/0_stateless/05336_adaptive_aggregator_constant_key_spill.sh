#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Constant-key blocks look up one key for all their rows. Hits can grow heap or arena states even
# after per-row probing is bypassed, so they count toward the frozen-table spill guard. Each shape
# compares its result with ordinary aggregation and reports whether a frozen table was spilled.
# Fixed states and key-only tables keep their data in memory because their hits do not grow it.
function run_shape()
{
    local label=$1 query=$2
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1}
    echo "$label"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000;
    SET collect_hash_table_stats_during_aggregation = 0, optimize_group_by_constant_keys = 1;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

    SELECT 'matches the baseline', (${off}) = (${on});
    SELECT 'frozen tables written to disk',
        (SELECT coalesce(sum(value), 0) FROM system.events
         WHERE event = 'AdaptiveAggregationFrozenTableSpills') > 0;
    "
}

run_shape 'Heap states' "SELECT a FROM (
    SELECT identity(toUInt64(1)) AS k, uniqExact(number) AS a
    FROM numbers_mt(1000000) GROUP BY k SETTINGS __SETTINGS__)"

run_shape 'Arena states' "SELECT length(a), arraySum(a) FROM (
    SELECT identity(toUInt64(1)) AS k, groupArray(number) AS a
    FROM numbers_mt(1000000) GROUP BY k SETTINGS __SETTINGS__)"

run_shape 'Fixed states' "SELECT a FROM (
    SELECT identity(toUInt64(1)) AS k, sum(number) AS a
    FROM numbers_mt(1000000) GROUP BY k SETTINGS __SETTINGS__)"

run_shape 'Inline count' "SELECT a FROM (
    SELECT identity(toUInt64(1)) AS k, count() AS a
    FROM numbers_mt(1000000) GROUP BY k SETTINGS __SETTINGS__)"

run_shape 'Key-only table' "SELECT count() FROM (
    SELECT identity(toUInt64(1)) AS k FROM numbers_mt(1000000)
    GROUP BY k SETTINGS __SETTINGS__)"

run_shape 'Key-only string table' "SELECT count() FROM (
    SELECT identity('key') AS k FROM numbers_mt(1000000)
    GROUP BY k SETTINGS __SETTINGS__)"
