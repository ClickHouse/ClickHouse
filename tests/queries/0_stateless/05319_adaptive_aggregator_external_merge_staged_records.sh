#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a thread of the adaptive aggregator on the baseline path spills its table as an ordinary part, the merge goes
# external, and the records the frozen threads staged join it as inputs of their own: one source per merging thread
# merges its share of the buckets into chunks of aggregate states, reading the staged records of a bucket from memory
# and from the spill streams of the bucket's partitions.
#
# The streams repeat every key a hundred times, so the frozen tables stage records until the thaw verdict sends every
# thread back to the baseline path. A thread spills its staged records when they hold an eighth of the threshold. With
# a small external-aggregation threshold the threads spill staged records dozens of times before the thaw, and the
# thawed two-level tables spill parts after it, so the first cells merge spilled staged records and parts together.
# The records a thread staged after its last spill reach the merge from memory or not, depending on how the blocks
# fall between the spills and the thaw, so these cells only check that staged records were spilled. In the last cell
# the repeated keys are followed by unique ones: the threads cannot all reach an eighth of the threshold with the
# records staged before the thaw, so staged records reach the merge from memory, while the thawed tables grow past the
# threshold. One thread may still spill, if it takes a large share of the blocks, so this cell checks that fewer
# records were spilled than staged. Every cell compares the result with the one of the feature off, checks that the
# thaw and the external merge happened, and checks where the merge read the staged records from. Each cell runs in its
# own clickhouse-local process, so the counters in `system.events` belong to it alone.

staged_records="(SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords')"
spilled_records="(SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords')"

function check()
{
    local label=$1 threshold=$2 rows=$3 key=$4 aggregate=$5 staged_records_source=$6
    local query="SELECT count(), sum(cityHash64(k, s)) FROM (SELECT $key AS k, $aggregate AS s FROM numbers_mt($rows) GROUP BY k)"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET collect_hash_table_stats_during_aggregation = 0, group_by_two_level_threshold = 1000, group_by_two_level_threshold_bytes = 1000000;
    SET max_bytes_before_external_group_by = $threshold, max_bytes_ratio_before_external_group_by = 0, enable_adaptive_aggregator = 1;

    SELECT '$label', ($query) = ($query SETTINGS enable_adaptive_aggregator = 0, max_bytes_before_external_group_by = 0);
    SELECT
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationMerge') > 0,
        $staged_records_source;
    " | paste -sd '\t'
}

check 'UInt64 key, fixed-width argument' 2000000 2000000 'toUInt64(number % 20000)' 'sum(number)' "$spilled_records > 0"
check 'String key, count' 2000000 2000000 "concat('key_', toString(number % 20000))" 'count()' "$spilled_records > 0"
check 'UInt64 key, variable-width argument' 2000000 2000000 'toUInt64(number % 20000)' 'max(toString(number))' \
    "$spilled_records > 0"
check 'String key, states with destructors' 2000000 2000000 "concat('key_', toString(number % 20000))" \
    'uniqExact(number % 97)' "$spilled_records > 0"
check 'Staged records in memory' 64000000 6000000 'if(number < 2000000, number % 20000, number)' 'sum(number)' \
    "$staged_records > $spilled_records"
