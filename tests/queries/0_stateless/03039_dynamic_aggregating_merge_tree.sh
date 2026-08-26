#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Fix some settings to avoid timeouts because of some settings randomization
CH_CLIENT="$CLICKHOUSE_CLIENT --allow_merge_tree_settings --allow_experimental_dynamic_type=1 --index_granularity_bytes 10485760 --index_granularity 8128 --merge_max_block_size 8128 --optimize_aggregation_in_order 0"

function test()
{
    $CH_CLIENT -q "create table test (id UInt64, sum AggregateFunction(sum, UInt64), d Dynamic) engine=AggregatingMergeTree() order by id settings $1;"
    $CH_CLIENT -q "system stop merges test"
    $CH_CLIENT -q "insert into test select number, sumState(1::UInt64), number from numbers(100000) group by number"
    $CH_CLIENT -q "insert into test select number, sumState(1::UInt64), 'str_' || toString(number) from numbers(50000, 100000) group by number"

    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select count(), sum from (select sumMerge(sum) as sum from test group by id, _part) group by sum order by sum, count()"
    $CH_CLIENT -m -q "system start merges test; optimize table test final"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select count(), sum from (select sumMerge(sum) as sum from test group by id, _part) group by sum order by sum, count()"
    $CH_CLIENT -q "drop table test"
}

$CH_CLIENT -q "drop table if exists test;"

echo "MergeTree compact + horizontal merge"
test "min_rows_for_wide_part=100000000000, min_bytes_for_wide_part=1000000000000, vertical_merge_algorithm_min_rows_to_activate=10000000000, vertical_merge_algorithm_min_columns_to_activate=100000000000"

echo "MergeTree wide + horizontal merge"
test "min_rows_for_wide_part=1, min_bytes_for_wide_part=1,vertical_merge_algorithm_min_rows_to_activate=1000000000, vertical_merge_algorithm_min_columns_to_activate=1000000000000"

echo "MergeTree compact + vertical merge"
test "min_rows_for_wide_part=100000000000, min_bytes_for_wide_part=1000000000000, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1"

echo "MergeTree wide + vertical merge"
test "min_rows_for_wide_part=1, min_bytes_for_wide_part=1, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1"
