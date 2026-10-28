#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A count-first unit of an adaptive aggregation that feeds `ORDER BY count() DESC LIMIT n` keeps its best
# groups while it counts. It finds a group's entry through its count in the counting table and confirms it by
# the key, so every key method takes its own path: numbers, packed strings, strings in every submap of the
# legacy string table, serialized keys and wide fixed keys. A unit whose records spilled reads them back twice,
# once to count and once to build the states of its best groups, a block at a time, so the counting table and
# the best groups copy the string keys they keep out of the blocks. The spilled string cells repeat the string
# key methods spilling on every block, so most groups enter the counting table from a spilled block, under a
# throw-mode group limit at the exact group count: a key the counting table did not copy splits its group,
# which exceeds the limit. The limit also turns the top-K pruning off, so every unit is counted.
#
# Half of the rows go to a head of 3000 keys with about 25 rows each, so many groups tie around the top and
# replace each other among the best. The other half are unique keys, which keep the aggregation frozen. A
# freeze threshold above the head keeps most head keys in the frozen tables as source cells; a small one
# stages most of their rows as records. Every cell compares the top with the top of the feature off: the
# counts as a multiset, which ties cannot reorder, and every returned row against the row of its key in the
# full aggregation. The last two columns say whether units were merged count first and whether records were
# spilled. Each cell runs in its own clickhouse-local process, so the counters in `system.events` belong to
# it alone.

function check()
{
    local label=$1 settings=$2 keys=$3 columns=$4 order=$5
    local source="SELECT if(number % 2 = 0, intHash64(number) % 3000, number + 1000000000) AS k, number FROM numbers_mt(150000)"
    local query="SELECT $keys, count() AS c, sum(number) AS s FROM ($source) GROUP BY $columns ORDER BY $order"
    local full="SELECT $keys, count() AS c, sum(number) AS s FROM ($source) GROUP BY $columns SETTINGS enable_adaptive_aggregator = 0"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192, adaptive_aggregator_freeze_threshold = 128;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1;
    SET $settings;

    SELECT '$label',
        (SELECT arraySort(groupArray(c)) FROM ($query))
            = (SELECT arraySort(groupArray(c)) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT count() FROM ($query) WHERE ($columns, c, s) NOT IN ($full)) = 0,
        (SELECT count() FROM ($query));
    SELECT
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationCountFirstUnits') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpills') > 0;
    " | paste -sd '\t'
}

legacy_string_key="concat(toString(k), repeat('x', k % 30), if(k % 11 = 0, '\\0', '')) AS sk"
spilled_string_settings="max_bytes_before_external_group_by = 1, max_rows_to_group_by = 78000, group_by_overflow_mode = 'throw'"

check 'Number key' 'max_threads = 4' 'k' 'k' 'c DESC LIMIT 10'
check 'Number key in source cells' 'adaptive_aggregator_freeze_threshold = 8192' 'k' 'k' 'c DESC LIMIT 10'
check 'Packed string key' 'max_threads = 4' "concat(toString(k), repeat('x', k % 30)) AS sk" 'sk' 'c DESC LIMIT 10'
check 'Legacy string key' 'enable_packed_string_keys_in_aggregation = 0' "$legacy_string_key" 'sk' 'c DESC LIMIT 10'
check 'Legacy string key in source cells' 'enable_packed_string_keys_in_aggregation = 0, adaptive_aggregator_freeze_threshold = 8192' "$legacy_string_key" 'sk' 'c DESC LIMIT 10'
check 'Serialized keys' 'max_threads = 4' 'k % 100 AS a, toString(intDiv(k, 100)) AS b' 'a, b' 'c DESC LIMIT 10'
check 'Wide fixed keys' 'max_threads = 4' 'toUInt64(k % 100) AS a, toUInt64(intDiv(k, 100)) AS b' 'a, b' 'c DESC LIMIT 10'
check 'Full large top' 'max_threads = 4' 'k' 'k' 'c DESC LIMIT 300 OFFSET 100'
check 'Limit above the groups of a unit' 'max_threads = 4' 'k' 'k' 'c DESC LIMIT 65536'
check 'Spilled number key' 'max_bytes_before_external_group_by = 2000000' 'k' 'k' 'c DESC LIMIT 10'
check 'Spilled packed string key' "$spilled_string_settings" "concat(toString(k), repeat('x', k % 30)) AS sk" 'sk' 'c DESC LIMIT 10'
check 'Spilled legacy string key' "enable_packed_string_keys_in_aggregation = 0, $spilled_string_settings" "$legacy_string_key" 'sk' 'c DESC LIMIT 10'
check 'Spilled serialized keys' "$spilled_string_settings" 'k % 100 AS a, toString(intDiv(k, 100)) AS b' 'a, b' 'c DESC LIMIT 10'
