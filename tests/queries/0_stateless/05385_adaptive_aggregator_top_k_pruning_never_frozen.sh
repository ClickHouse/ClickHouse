#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The top-K pruning of an adaptive aggregation bounds the rank of every group by the counts in its bin. A
# producer that froze counts its staged records and its own table into its bins. A producer that never froze
# staged nothing and has no bins, so the merge counts the groups of its table into the bounds of their buckets.
#
# One stream freezes: a head of ten keys with 1000 rows each, then a tail of unique keys. Four streams that
# never freeze hold the top: five keys with 2500 rows per stream, none of them in the frozen stream. Every
# stream is one block, so each goes to its own producer, and `max_threads_min_free_memory_per_thread = 0` keeps
# the eight threads whatever the free memory. The merge takes the buckets of the head first, and
# their counts raise the threshold to 1000. The frozen producer's bins hold only a few tail rows in the bins of
# the top, so without the tables of the streams that never froze the merge would skip the units of the top.
# Every cell compares the top with the one of the same query with the feature off, and says whether exactly one
# producer froze and whether the merge skipped units. `optimize_injective_functions_in_group_by = 0` keeps
# `toString(k)` a string key. Each cell runs in its own clickhouse-local process, so the counters in
# `system.events` belong to it alone.

function check()
{
    local label=$1 keys=$2 aggregates=$3 order=$4 columns=$5
    local small="SELECT toUInt64(100 + number % 5) AS k, number FROM numbers(12500)"
    local source="SELECT if(number < 10000, number % 10, number + 1000000) AS k, number FROM numbers(300000)
        UNION ALL $small UNION ALL $small UNION ALL $small UNION ALL $small"
    local query="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys ORDER BY $order"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 8, max_threads_min_free_memory_per_thread = 0, max_block_size = 300000;
    SET adaptive_aggregator_freeze_threshold = 1000;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SET optimize_injective_functions_in_group_by = 0;
    SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1;

    SELECT '$label',
        (SELECT arraySort(groupArray(($columns))) FROM ($query))
            = (SELECT arraySort(groupArray(($columns))) FROM ($query SETTINGS enable_adaptive_aggregator = 0));
    SELECT (SELECT sum(value) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') = 1,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedUnits') > 0;
    " | paste -sd '\t'
}

check 'UInt64 key, count' 'k' 'count() AS c' 'c DESC LIMIT 5' 'k, c'
check 'String key, count' 'toString(k) AS sk' 'count() AS c' 'c DESC LIMIT 5' 'sk, c'
check 'Count among other aggregates' 'k' 'count() AS c, sum(number) AS s' 'c DESC LIMIT 5' 'k, c, s'
check 'uniqExact' 'k' 'uniqExact(number) AS u' 'u DESC LIMIT 5' 'k, u'
