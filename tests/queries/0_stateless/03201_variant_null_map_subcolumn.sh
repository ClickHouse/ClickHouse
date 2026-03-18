#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --use_variant_as_common_type=1 --allow_suspicious_variant_types=1"

function test()
{
    echo "test"
    $CH_CLIENT -q "insert into test select number, multiIf(number % 3 == 2, NULL, number % 3 == 1, number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10))) from numbers(36)"
    $CH_CLIENT -q "select v, v.UInt64.null, v.\`Array(Variant(String, UInt64))\`.null,  v.\`Array(Variant(String, UInt64))\`.size0, v.\`Array(Variant(String, UInt64))\`.UInt64.null from test order by id"
    $CH_CLIENT -q "select v.UInt64.null, v.\`Array(Variant(String, UInt64))\`.null,  v.\`Array(Variant(String, UInt64))\`.size0, v.\`Array(Variant(String, UInt64))\`.UInt64.null from test order by id"
    $CH_CLIENT -q "select v.\`Array(Variant(String, UInt64))\`.null,  v.\`Array(Variant(String, UInt64))\`.size0, v.\`Array(Variant(String, UInt64))\`.UInt64.null, v.\`Array(Variant(String, UInt64))\`.String.null from test order by id"
    $CH_CLIENT -q "select id from test where v.UInt64 is null order by id"
    $CH_CLIENT -q "insert into test select number, multiIf(number % 3 == 2, NULL, number % 3 == 1, number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10))) from numbers(250000) settings min_insert_block_size_rows=100000, min_insert_block_size_bytes=0"
    $CH_CLIENT -q "select v, v.UInt64.null, v.\`Array(Variant(String, UInt64))\`.null,  v.\`Array(Variant(String, UInt64))\`.size0, v.\`Array(Variant(String, UInt64))\`.UInt64.null from test order by id format Null"
    $CH_CLIENT -q "select v.UInt64.null, v.\`Array(Variant(String, UInt64))\`.null,  v.\`Array(Variant(String, UInt64))\`.size0, v.\`Array(Variant(String, UInt64))\`.UInt64.null from test order by id format Null"
    $CH_CLIENT -q "select v.\`Array(Variant(String, UInt64))\`.null,  v.\`Array(Variant(String, UInt64))\`.size0, v.\`Array(Variant(String, UInt64))\`.UInt64.null, v.\`Array(Variant(String, UInt64))\`.String.null from test order by id format Null"
    $CH_CLIENT -q "select id from test where v.UInt64 is null order by id format Null"
}

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, Array(Variant(String, UInt64)))) engine=Memory"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, Array(Variant(String, UInt64)))) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, Array(Variant(String, UInt64)))) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
test
$CH_CLIENT -q "drop table test;"
