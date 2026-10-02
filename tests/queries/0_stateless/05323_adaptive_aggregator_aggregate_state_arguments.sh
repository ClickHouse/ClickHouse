#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive aggregator stages the arguments of the rows its frozen tables miss in records, and an argument whose
# serialized size it cannot know in advance it serializes through the arena. The states of an `AggregateFunction`
# column are such arguments: a `-Merge` aggregate over them stages every state it misses, and the merge deserializes
# it back into a column for the aggregate.
#
# The inner query builds a state per key, and the outer one merges the states of four inner keys per group, in blocks
# small enough for the outer tables to freeze. The outer key stays a `String`, not reduced to the `UInt16` argument of
# its injective conversion, whose fixed single-level table the adaptive aggregator does not take. Every cell compares
# the result with the one of the feature off and checks that records were staged and whether they were spilled: the
# last cell spills the staged states and reads them back. Each cell runs in its own clickhouse-local process, so the
# counters in `system.events` belong to it alone.

function check()
{
    local label=$1 settings=$2 inner=$3 outer=$4 result=$5
    local query="SELECT toString(k % 5000) AS g, $outer FROM (SELECT number % 20000 AS k, $inner FROM numbers_mt(4000000) GROUP BY k) GROUP BY g"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 3, max_block_size = 1000, adaptive_aggregator_freeze_threshold = 128;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SET enable_adaptive_aggregator = 1, optimize_injective_functions_in_group_by = 0;
    SET $settings;

    SELECT '$label',
        (SELECT sum(cityHash64(g, $result)) FROM ($query))
            = (SELECT sum(cityHash64(g, $result)) FROM ($query SETTINGS enable_adaptive_aggregator = 0));
    SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords') > 0;
    " | paste -sd '\t'
}

check 'uniqMerge' 'max_threads = 3' 'uniqState(number) AS s' 'uniqMerge(s) AS m' 'm'
check 'sumMerge' 'max_threads = 3' 'sumState(number) AS s' 'sumMerge(s) AS m' 'm'
check 'uniqMergeState' 'max_threads = 3' 'uniqState(number) AS s' 'uniqMergeState(s) AS m' 'finalizeAggregation(m)'
check 'Spilled states' 'max_bytes_before_external_group_by = 4000000' 'uniqState(number) AS s' 'uniqMerge(s) AS m' 'm'
