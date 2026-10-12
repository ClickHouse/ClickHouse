#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# One group retains enough distinct values to use the group merge, while the other groups have small states.
# The extra aggregates exercise state offsets in the ordinary merge, the count-first merge, and the global merge.
function check()
{
    local label=$1 aggregate=$2 input=$3 result=$4
    echo "$label"
    for suffix in 'ORDER BY k' 'ORDER BY c DESC LIMIT 1' 'without grouping'
    do
        local key='k' grouping="GROUP BY k ${suffix}"
        if [ "$suffix" = 'without grouping' ]
        then
            key='0 AS k'
            grouping=''
        fi
        for adaptive in 0 1
        do
            $CLICKHOUSE_LOCAL --query "
                SET max_threads = 4, max_block_size = 8192;
                SET collect_hash_table_stats_during_aggregation = 0;
                SET adaptive_aggregator_freeze_threshold = 2, adaptive_aggregator_freeze_threshold_bytes = 0;
                SET adaptive_aggregator_disable_thaw = 1;
                SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
                SET enable_adaptive_aggregator = ${adaptive};
                SELECT k, s, c, ${result} FROM (
                    SELECT ${key}, sum(n) AS s, count() AS c, ${aggregate} AS u
                    FROM (${input}) ${grouping});
            " > "${CLICKHOUSE_TMP}/wrapped_state_merge_${adaptive}.out"
        done
        diff -u "${CLICKHOUSE_TMP}/wrapped_state_merge_0.out" "${CLICKHOUSE_TMP}/wrapped_state_merge_1.out"
    done
}

input='SELECT number AS n, if(number % 4 < 3, 0, 1 + number % 100) AS k FROM numbers_mt(200000)'
check 'If' 'uniqExactIf(n, n % 2 = 0)' "$input" 'u'
check 'Nullable' 'uniqExact(if(n % 2 = 0, n, NULL))' "$input" 'u'
check 'Variadic' 'uniqExact(n, n % 7)' "$input" 'u'
check 'Tuple argument' 'uniqExact(tuple(n, n % 7))' "$input" 'u'
check 'Array' 'uniqExactArray([n])' "$input" 'u'
check 'State' 'uniqExactState(n)' "$input" 'finalizeAggregation(u)'
check 'ArrayIfState' 'uniqExactArrayIfState([n], n % 2 = 0)' "$input" 'finalizeAggregation(u)'

merge_input='SELECT inner_k % 2 AS k, inner_k AS n, state FROM (
    SELECT number % 100 AS inner_k, uniqExactState(number) AS state
    FROM numbers_mt(200000) GROUP BY inner_k)'
check 'Merge' 'uniqExactMerge(state)' "$merge_input" 'u'
