#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An adaptive aggregation that feeds `ORDER BY uniqExact(...) DESC LIMIT n`, which `COUNT(DISTINCT)` resolves to, or the
# same by `uniqExactIf`, which the analyzer makes of `uniqExact(if(...))`, bounds the distinct counts with the count
# bins of the top-K pruning: every staged record adds one to its bin, and at its
# finish every producer adds the distinct count of each group of its own table. A row raises a distinct count by one
# at most, and a merged group has no more distinct values than the groups merged into it together, so summed over the
# producers a bin bounds the distinct count of every group in it, and once the merge knows `n` exact distinct counts it
# skips the merge units, staged records and source cells of the bins bounded below the smallest of them.
#
# The source puts a head of 100 keys, whose rows grow with the key, in front of a tail of unique keys, so the top is a
# few groups with thousands of distinct values and almost every bin is bounded far below them. Every cell compares the
# top with the one of the feature off: the distinct counts as a multiset, which ties cannot reorder, and every returned
# row against the row of its key in the full aggregation, which catches a group whose count lost records or source
# cells. The last two columns say whether the merge skipped units and records; an ascending order must not prune. Each
# cell runs in its own clickhouse-local process, so the counters in `system.events` belong to it alone.

function check()
{
    local label=$1 settings=$2 keys=$3 aggregates=$4 order=$5
    local source="SELECT if(number % 4 = 0, toUInt64(sqrt(number % 10000)), number + 1000000) AS k, number FROM numbers_mt(1000000)"
    local query="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys ORDER BY $order"
    local full="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys SETTINGS enable_adaptive_aggregator = 0"
    local columns
    columns="$(echo "$keys" | sed -E 's/.* AS ([a-z]+)$/\1/'), $(echo "$aggregates" | grep -oE ' AS [a-z]+' | sed 's/ AS //' | paste -sd ',' | sed 's/,/, /g')"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192, adaptive_aggregator_freeze_threshold = 128, count_distinct_implementation = 'uniqExact';
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1;
    SET $settings;

    SELECT '$label',
        (SELECT arraySort(groupArray(u)) FROM ($query))
            = (SELECT arraySort(groupArray(u)) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT count() FROM ($query) WHERE ($columns) NOT IN ($full)) = 0;
    SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedUnits') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedRecords') > 0;
    " | paste -sd '\t'
}

check 'UInt64 key' 'max_threads = 4' 'k' 'uniqExact(number) AS u' 'u DESC LIMIT 10'
check 'COUNT(DISTINCT)' 'max_threads = 4' 'k' 'count(DISTINCT number) AS u' 'u DESC LIMIT 10'
check 'String key' 'max_threads = 4' 'toString(k) AS sk' 'uniqExact(number) AS u' 'u DESC LIMIT 10'
check 'With other aggregates' 'max_threads = 4' 'k' 'count() AS c, uniqExact(number % 1000) AS u, sum(number) AS s' 'u DESC LIMIT 10'
check 'Nullable argument' 'max_threads = 4' 'k' 'uniqExact(nullIf(number, 4)) AS u' 'u DESC LIMIT 10'
check 'uniqExactIf' 'max_threads = 4' 'k' 'uniqExact(if(number % 3 = 0, NULL, number)) AS u' 'u DESC LIMIT 10'
check 'Two arguments' 'max_threads = 4' 'k' 'uniqExact(number % 3, number) AS u' 'u DESC LIMIT 10'
check 'Spilled records' 'max_bytes_before_external_group_by = 8000000' 'k' 'uniqExact(number) AS u' 'u DESC LIMIT 10'
check 'Ascending order' 'max_threads = 4' 'k' 'uniqExact(number) AS u' 'u ASC LIMIT 10'
