#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An adaptive aggregation that feeds `ORDER BY count() DESC LIMIT n` counts the rows of every staged record, and of
# every producer's own table, into bins on the bits of the key's hash below and including the bucket's. Summed over the
# producers, a bin bounds the count of every group in it from above, so once the merge knows `n` exact counts it skips
# every merge unit, staged record and source cell of a bin bounded below the smallest of them: such a group has `n`
# groups ahead of it.
#
# The source puts a head of 100 keys, whose counts grow with the key, in front of a tail of unique keys, so the top is
# a few heavy groups and almost every bin is bounded far below them. Every cell compares the top with the one of the
# same query with the feature off: the counts as a multiset, which ties cannot reorder, and every returned row against
# the row of its key in the full aggregation, which catches a group whose count lost records or source cells. The last
# two columns say whether the merge skipped units and records. The cells for an ascending order, a throw-mode group
# limit and `WITH TIES` must not prune. Each cell runs in its own clickhouse-local process, so the counters in
# `system.events` belong to it alone.

function check()
{
    local label=$1 settings=$2 keys=$3 aggregates=$4 order=$5 rows=${6:-1000000}
    local source="SELECT if(number % 4 = 0, toUInt64(sqrt(number % 10000)), number + 1000000) AS k, number FROM numbers_mt($rows)"
    local query="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys ORDER BY $order"
    local full="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys SETTINGS enable_adaptive_aggregator = 0"
    local columns
    columns=$(echo "$keys, $aggregates" | sed -E 's/[^,]* AS ([a-z]+)/\1/g')
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192, adaptive_aggregator_freeze_threshold = 128;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1;
    SET $settings;

    SELECT '$label',
        (SELECT arraySort(groupArray(c)) FROM ($query))
            = (SELECT arraySort(groupArray(c)) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT count() FROM ($query) WHERE ($columns) NOT IN ($full)) = 0;
    SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedUnits') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedRecords') > 0;
    " | paste -sd '\t'
}

check 'UInt64 key, count' 'max_threads = 4' 'k' 'count() AS c' 'c DESC LIMIT 10'
check 'String key, count' 'max_threads = 4' 'toString(k) AS sk' 'count() AS c' 'c DESC LIMIT 10'
check 'Two keys, count' 'max_threads = 4' 'k, k % 3 AS r' 'count() AS c' 'c DESC LIMIT 10'
check 'Fixed-width arguments' 'max_threads = 4' 'k' 'count() AS c, sum(number) AS s, avg(number % 7) AS a' 'c DESC LIMIT 10'
check 'Variable-width argument' 'max_threads = 4' 'toString(k) AS sk' 'count() AS c, max(toString(number)) AS m' 'c DESC LIMIT 10'
check 'States with destructors' 'max_threads = 4' 'k' 'count() AS c, uniqExact(number % 97) AS u' 'c DESC LIMIT 10'
check 'Offset' 'max_threads = 4' 'k' 'count() AS c' 'c DESC LIMIT 5 OFFSET 3'
check 'Spilled records' 'max_bytes_before_external_group_by = 8000000' 'k' 'count() AS c, sum(number) AS s' 'c DESC LIMIT 10'
check 'Streams that never froze' 'max_threads = 16' 'k' 'count() AS c' 'c DESC LIMIT 10' 60000
check 'Ascending order' 'max_threads = 4' 'k' 'count() AS c' 'c ASC LIMIT 10'
check 'Throw-mode group limit' "max_rows_to_group_by = 10000000, group_by_overflow_mode = 'throw'" 'k' 'count() AS c' 'c DESC LIMIT 10'
check 'With ties' 'max_threads = 4' 'k' 'count() AS c' 'c DESC LIMIT 10 WITH TIES'
