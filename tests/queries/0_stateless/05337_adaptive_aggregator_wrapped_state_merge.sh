#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# One group retains enough distinct values to use the group merge, while the other groups have small states.
# The extra aggregates exercise state offsets in the ordinary merge, the count-first merge, and the global merge.
# The keys are `UInt64`: a key of at most two bytes has no two-level form, so the adaptive aggregator would not engage.
# The largest group of every input has a count of its own, because the groups that tie on `ORDER BY c DESC LIMIT 1` may
# come out in another order from the adaptive merge. Both runs of a cell share a clickhouse-local process, the adaptive
# one first, so the counters in `system.events` belong to it alone: the run with the feature off adds none of them. For
# the grouped cells, the adaptive run reports whether its tables froze, whether its merge converted units, and whether
# it counted the groups first.
function check()
{
    local label=$1 aggregate=$2 input=$3 result=$4
    echo "$label"
    for suffix in 'ORDER BY k' 'ORDER BY c DESC LIMIT 1' 'without grouping'
    do
        local key='k' grouping="GROUP BY k ${suffix}" engagement=''
        if [ "$suffix" = 'without grouping' ]
        then
            key='0 AS k'
            grouping=''
        else
            engagement="SELECT 'adaptive, ${suffix}', sumIf(value, event = 'AdaptiveAggregationLocalFreezes') > 0,
                sumIf(value, event = 'AdaptiveAggregationMergeUnits') > 0,
                sumIf(value, event = 'AdaptiveAggregationCountFirstUnits') > 0 FROM system.events;"
        fi
        local query="SELECT k, s, c, ${result} FROM (
            SELECT ${key}, sum(n) AS s, count() AS c, ${aggregate} AS u
            FROM (${input}) ${grouping})"
        $CLICKHOUSE_LOCAL --query "
            SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
            SET collect_hash_table_stats_during_aggregation = 0;
            SET adaptive_aggregator_freeze_threshold = 2, adaptive_aggregator_freeze_threshold_bytes = 0;
            SET adaptive_aggregator_disable_thaw = 1;
            SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
            ${query} SETTINGS enable_adaptive_aggregator = 1
                INTO OUTFILE '${CLICKHOUSE_TMP}/wrapped_state_merge_1.out' TRUNCATE;
            ${query} SETTINGS enable_adaptive_aggregator = 0
                INTO OUTFILE '${CLICKHOUSE_TMP}/wrapped_state_merge_0.out' TRUNCATE;
            ${engagement}
        "
        diff -u "${CLICKHOUSE_TMP}/wrapped_state_merge_0.out" "${CLICKHOUSE_TMP}/wrapped_state_merge_1.out"
    done
}

input='SELECT number AS n, toUInt64(if(number % 4 < 3, 0, 1 + number % 100)) AS k FROM numbers_mt(200000)'
check 'If' 'uniqExactIf(n, n % 2 = 0)' "$input" 'u'
check 'Nullable' 'uniqExact(if(n % 2 = 0, n, NULL))' "$input" 'u'
check 'Variadic' 'uniqExact(n, n % 7)' "$input" 'u'
check 'Tuple argument' 'uniqExact(tuple(n, n % 7))' "$input" 'u'
check 'Array' 'uniqExactArray([n])' "$input" 'u'
check 'State' 'uniqExactState(n)' "$input" 'finalizeAggregation(u)'
check 'ArrayIfState' 'uniqExactArrayIfState([n], n % 2 = 0)' "$input" 'finalizeAggregation(u)'

merge_input='SELECT toUInt64(inner_k % 4 = 0) AS k, inner_k AS n, state FROM (
    SELECT number % 100 AS inner_k, uniqExactState(number) AS state
    FROM numbers_mt(200000) GROUP BY inner_k)'
check 'Merge' 'uniqExactMerge(state)' "$merge_input" 'u'
