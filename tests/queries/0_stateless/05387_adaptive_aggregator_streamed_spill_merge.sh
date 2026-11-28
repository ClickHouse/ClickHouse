#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge unit of the adaptive aggregator reads the spill streams of its partitions back a block at a time and drains
# each block before it reads the next, so its table copies the string-like keys it keeps out of the block. Every shape
# repeats its keys many times, so most records of a key meet the key already in the table, and spills under a
# threshold of one byte, which writes the staged records to disk on every block. The shapes cover the record formats
# and the string key methods: key-only records with packed string keys, argument records with legacy string keys of
# every length, so every submap of the string table takes keys, argument records with serialized keys, and count
# records with packed string keys. The string keys are concatenations of two columns, which the optimizer cannot
# replace by their arguments as it replaces an injective function, and most are long enough that a packed key keeps its
# bytes out of line. Every shape compares its result with the same query with the feature off and no spilling, and
# prints whether staged records spilled. The thaw is off, so the repeated keys stay staged, and the thresholds are
# pinned because the runner randomizes them. Each shape runs in its own clickhouse-local process, so the counters in
# `system.events` belong to it alone.
function run_shape()
{
    local query=$1
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1}
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

    SELECT 'matches the baseline', (${off}) = (${on});
    SELECT 'staged records spilled', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpills') > 0;
    "
}

echo 'Key-only records, packed string keys'
run_shape "SELECT count(), sum(cityHash64(k)) FROM (
    SELECT concat(toString(number % 50000), repeat('k', number % 40)) AS k FROM numbers_mt(1000000) GROUP BY k SETTINGS __SETTINGS__)"

echo 'Argument records, legacy string keys of every length'
run_shape "SELECT count(), sum(cityHash64(k)), sum(c), sum(m) FROM (
    SELECT concat(repeat('x', number % 30), toString(number % 50000)) AS k, count() AS c, max(number) AS m FROM numbers_mt(1000000)
    GROUP BY k SETTINGS enable_packed_string_keys_in_aggregation = 0, __SETTINGS__)"

echo 'Argument records, serialized keys'
run_shape "SELECT count(), sum(cityHash64(a, b)), sum(s) FROM (
    SELECT concat(toString(number % 50000), repeat('s', number % 20)) AS a, number % 3 AS b, sum(number) AS s FROM numbers_mt(1000000)
    GROUP BY a, b SETTINGS __SETTINGS__)"

echo 'Count records, packed string keys'
run_shape "SELECT count(), sum(cityHash64(k)), sum(c) FROM (
    SELECT concat(toString(number % 50000), repeat('y', number % 50)) AS k, count() AS c FROM numbers_mt(1000000)
    GROUP BY k SETTINGS __SETTINGS__)"
