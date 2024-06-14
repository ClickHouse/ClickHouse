#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1"


function test()
{
    echo "test"
    $CH_CLIENT -q "system stop merges test"
    $CH_CLIENT -q "insert into test select number, number from numbers(1000000)"
    $CH_CLIENT -q "insert into test select number, 'str_' || toString(number) from numbers(1000000, 1000000)"
    $CH_CLIENT -q "insert into test select number, range(number % 10 + 1) from numbers(2000000, 1000000)"

    $CH_CLIENT -nm -q "system start merges test; optimize table test final;"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
}

$CH_CLIENT -q "drop table if exists test;"

echo "MergeTree compact + horizontal merge"
$CH_CLIENT -q "create table test (id UInt64, d Dynamic) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide + horizontal merge"
$CH_CLIENT -q "create table test (id UInt64, d Dynamic) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact + vertical merge"
$CH_CLIENT -q "create table test (id UInt64, d Dynamic) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide + vertical merge"
$CH_CLIENT -q "create table test (id UInt64, d Dynamic) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;"
test
$CH_CLIENT -q "drop table test;"
