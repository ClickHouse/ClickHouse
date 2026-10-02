#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a thread of the adaptive aggregator on the baseline path spills its table as an ordinary part, the merge goes
# external, and the records the frozen threads staged join it as inputs of their own: one source per merging thread
# merges its share of the buckets into chunks of aggregate states, reading the staged records of a bucket from memory
# and from the bucket's spill stream.
#
# The streams repeat every key a hundred times, so the frozen tables stage records until the thaw verdict sends every
# thread back to the baseline path. With a small external-aggregation threshold the threads spill the staged records
# they hold before the thaw, and the thawed two-level tables spill parts after it, so the first cells merge spilled
# staged records and parts together. In the last cell the repeated keys are followed by unique ones: a thread spills
# its staged records only when they hold an eighth of the threshold, which the threads cannot all reach with the
# records staged before the thaw, so staged records reach the merge from memory, while the thawed tables grow past
# the threshold. Every cell compares the result with the one of the feature off, and checks that the thaw and the
# external merge happened; the last column says whether staged records reached the merge from memory. Each cell runs
# in its own clickhouse-local process, so the counters in `system.events` belong to it alone.

function check()
{
    local label=$1 threshold=$2 rows=$3 key=$4 aggregate=$5
    local query="SELECT count(), sum(cityHash64(k, s)) FROM (SELECT $key AS k, $aggregate AS s FROM numbers_mt($rows) GROUP BY k)"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET collect_hash_table_stats_during_aggregation = 0, group_by_two_level_threshold = 1000, group_by_two_level_threshold_bytes = 1000000;
    SET max_bytes_before_external_group_by = $threshold, max_bytes_ratio_before_external_group_by = 0, enable_adaptive_aggregator = 1;

    SELECT '$label', ($query) = ($query SETTINGS enable_adaptive_aggregator = 0, max_bytes_before_external_group_by = 0);
    SELECT
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationMerge') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords')
            > (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords');
    " | paste -sd '\t'
}

check 'UInt64 key, fixed-width argument' 2000000 2000000 'toUInt64(number % 20000)' 'sum(number)'
check 'String key, count' 2000000 2000000 "concat('key_', toString(number % 20000))" 'count()'
check 'UInt64 key, variable-width argument' 2000000 2000000 'toUInt64(number % 20000)' 'max(toString(number))'
check 'String key, states with destructors' 2000000 2000000 "concat('key_', toString(number % 20000))" 'uniqExact(number % 97)'
check 'Staged records in memory' 64000000 6000000 'if(number < 2000000, number % 20000, number)' 'sum(number)'
