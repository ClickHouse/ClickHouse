#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# Fix some settings to avoid timeouts because of some settings randomization
CH_CLIENT="$CLICKHOUSE_CLIENT --allow_merge_tree_settings --allow_experimental_dynamic_type=1  --index_granularity_bytes 10485760 --index_granularity 8128 --merge_max_block_size 8128"

function test()
{
    $CH_CLIENT -q "create table test (id UInt64, sign Int8, version UInt8, d Dynamic) engine=VersionedCollapsingMergeTree(sign, version) order by id settings $1;"
    $CH_CLIENT -q "system stop merges test"
    $CH_CLIENT -q "insert into test select number, 1, 1, number from numbers(100000)"
    $CH_CLIENT -q "insert into test select number, -1, number >= 75000 ? 2 : 1, 'str_' || toString(number) from numbers(50000, 100000)"

    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -m -q "system start merges test; optimize table test final"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "drop table test"
}

$CH_CLIENT -q "drop table if exists test;"

echo "MergeTree compact + horizontal merge"
test "min_rows_for_wide_part=100000000000, min_bytes_for_wide_part=1000000000000"

echo "MergeTree wide + horizontal merge"
test "min_rows_for_wide_part=1, min_bytes_for_wide_part=1"

echo "MergeTree compact + vertical merge"
test "min_rows_for_wide_part=100000000000, min_bytes_for_wide_part=1000000000000, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;"

echo "MergeTree wide + vertical merge"
test "min_rows_for_wide_part=1, min_bytes_for_wide_part=1, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;"
