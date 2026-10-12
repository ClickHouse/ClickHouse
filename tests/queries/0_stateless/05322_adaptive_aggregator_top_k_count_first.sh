#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An adaptive aggregation that feeds `ORDER BY count() LIMIT n` keeps only the best groups of every merge unit by the
# count in its final conversion, so when `count()` sits among other aggregates a unit counts its groups first, from
# the records and the count states of the source cells, and builds the other aggregate states only for its best
# groups: it drains just their records and adopts or merges just their source cells, and the other cells go with
# their states. The counts are exact, because a unit holds every record and source cell of its keys.
#
# The source puts a head of 100 keys, whose counts grow with the key, in front of a tail of unique keys, so the head
# keys sit in the frozen tables as source cells and the tail in the staged records. Every cell compares the top with
# the one of the feature off: the counts as a multiset, which ties cannot reorder, and every returned row against the
# row of its key in the full aggregation, which catches a group whose other aggregates missed records or source cells.
# The last column says whether units were merged count first. An ascending order and a throw-mode group limit, which
# turn the top-K pruning off, still count first; the group limit is checked against the groups the counting found, so
# the last query, whose limit is below them, fails although every unit keeps only its best groups. Each cell runs in
# its own clickhouse-local process, so the counters in `system.events` belong to it alone.

function check()
{
    local label=$1 settings=$2 keys=$3 aggregates=$4 order=$5
    local source="SELECT if(number % 4 = 0, toUInt64(sqrt(number % 10000)), number + 1000000) AS k, number FROM numbers_mt(1000000)"
    local query="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys ORDER BY $order"
    local full="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys SETTINGS enable_adaptive_aggregator = 0"
    local columns
    columns="$(echo "$keys" | sed -E 's/.* AS ([a-z]+)$/\1/'), $(echo "$aggregates" | grep -oE ' AS [a-z]+' | sed 's/ AS //' | paste -sd ',' | sed 's/,/, /g')"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192, adaptive_aggregator_freeze_threshold = 128;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1;
    SET $settings;

    SELECT '$label',
        (SELECT arraySort(groupArray(c)) FROM ($query))
            = (SELECT arraySort(groupArray(c)) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT count() FROM ($query) WHERE ($columns) NOT IN ($full)) = 0;
    SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationCountFirstUnits') > 0;
    " | paste -sd '\t'
}

check 'Fixed-width arguments' 'max_threads = 4' 'k' 'count() AS c, sum(number) AS s, avg(number % 7) AS a' 'c DESC LIMIT 10'
check 'Variable-width argument' 'max_threads = 4' 'k' 'count() AS c, max(toString(number)) AS m' 'c DESC LIMIT 10'
check 'States with destructors' 'max_threads = 4' 'k' 'count() AS c, uniqExact(number % 97) AS u' 'c DESC LIMIT 10'
check 'String key' 'max_threads = 4' 'toString(k) AS sk' 'count() AS c, sum(number) AS s' 'c DESC LIMIT 10'
check 'Offset' 'max_threads = 4' 'k' 'count() AS c, sum(number) AS s' 'c DESC LIMIT 5 OFFSET 3'
check 'Ascending order' 'max_threads = 4' 'k' 'count() AS c, sum(number) AS s' 'c ASC LIMIT 10'
check 'Spilled records' 'max_bytes_before_external_group_by = 8000000' 'k' 'count() AS c, sum(number) AS s' 'c DESC LIMIT 10'
check 'Throw-mode group limit' "max_rows_to_group_by = 10000000, group_by_overflow_mode = 'throw'" 'k' 'count() AS c, sum(number) AS s' 'c DESC LIMIT 10'

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_block_size = 8192, adaptive_aggregator_freeze_threshold = 128;
SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1;
SET max_rows_to_group_by = 100000, group_by_overflow_mode = 'throw';
SELECT k, count() AS c, sum(number) AS s
FROM (SELECT if(number % 4 = 0, toUInt64(sqrt(number % 10000)), number + 1000000) AS k, number FROM numbers_mt(1000000))
GROUP BY k ORDER BY c DESC LIMIT 10 FORMAT Null;
" 2>&1 | grep -o 'TOO_MANY_ROWS' | head -1
