#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --use_variant_as_common_type=1 --allow_experimental_dynamic_type=1"


function test()
{
    echo "test"
    $CH_CLIENT -q "insert into test select number, number from numbers(100000) settings min_insert_block_size_rows=50000"
    $CH_CLIENT -q "insert into test select number, 'str_' || toString(number) from numbers(100000, 100000) settings min_insert_block_size_rows=50000"
    $CH_CLIENT -q "insert into test select number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1)) from numbers(200000, 100000) settings min_insert_block_size_rows=50000"
    $CH_CLIENT -q "insert into test select number, NULL from numbers(300000, 100000) settings min_insert_block_size_rows=50000"
    $CH_CLIENT -q "insert into test select number, multiIf(number % 4 == 3, 'str_' || toString(number), number % 4 == 2, NULL, number % 4 == 1, number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1))) from numbers(400000, 400000) settings min_insert_block_size_rows=50000"
    $CH_CLIENT -q "insert into test select number, [range((number % 10 + 1)::UInt64)]::Array(Array(Dynamic)) from numbers(100000, 100000) settings min_insert_block_size_rows=50000"

    $CH_CLIENT -q "select distinct dynamicType(d) as type from test order by type"
    $CH_CLIENT -q "select count() from test where dynamicType(d) == 'UInt64'"
    $CH_CLIENT -q "select count() from test where d.UInt64 is not NULL"
    $CH_CLIENT -q "select count() from test where dynamicType(d) == 'String'"
    $CH_CLIENT -q "select count() from test where d.String is not NULL"
    $CH_CLIENT -q "select count() from test where dynamicType(d) == 'Date'"
    $CH_CLIENT -q "select count() from test where d.Date is not NULL"
    $CH_CLIENT -q "select count() from test where dynamicType(d) == 'Array(Variant(String, UInt64))'"
    $CH_CLIENT -q "select count() from test where not empty(d.\`Array(Variant(String, UInt64))\`)"
    $CH_CLIENT -q "select count() from test where dynamicType(d) == 'Array(Array(Dynamic))'"
    $CH_CLIENT -q "select count() from test where not empty(d.\`Array(Array(Dynamic))\`)"
    $CH_CLIENT -q "select count() from test where d is NULL"
    $CH_CLIENT -q "select count() from test where not empty(d.\`Tuple(a Array(Dynamic))\`.a.String)"

    $CH_CLIENT -q "select d, d.UInt64, d.String, d.\`Array(Variant(String, UInt64))\` from test format Null"
    $CH_CLIENT -q "select d.UInt64, d.String, d.\`Array(Variant(String, UInt64))\` from test format Null"
    $CH_CLIENT -q "select d.Int8, d.Date, d.\`Array(String)\` from test format Null"
    $CH_CLIENT -q "select d, d.UInt64, d.Date, d.\`Array(Variant(String, UInt64))\`, d.\`Array(Variant(String, UInt64))\`.size0, d.\`Array(Variant(String, UInt64))\`.UInt64 from test format Null"
    $CH_CLIENT -q "select d.UInt64, d.Date, d.\`Array(Variant(String, UInt64))\`, d.\`Array(Variant(String, UInt64))\`.size0, d.\`Array(Variant(String, UInt64))\`.UInt64, d.\`Array(Variant(String, UInt64))\`.String from test format Null"
    $CH_CLIENT -q "select d, d.\`Tuple(a UInt64, b String)\`.a, d.\`Array(Dynamic)\`.\`Variant(String, UInt64)\`.UInt64, d.\`Array(Variant(String, UInt64))\`.UInt64 from test format Null"
    $CH_CLIENT -q "select d.\`Array(Dynamic)\`.\`Variant(String, UInt64)\`.UInt64, d.\`Array(Dynamic)\`.size0, d.\`Array(Variant(String, UInt64))\`.UInt64 from test format Null"
    $CH_CLIENT -q "select d.\`Array(Array(Dynamic))\`.size1, d.\`Array(Array(Dynamic))\`.UInt64, d.\`Array(Array(Dynamic))\`.\`Map(String, Tuple(a UInt64))\`.values.a from test format Null"
}

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (id UInt64, d Dynamic) engine=Memory"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (id UInt64, d Dynamic) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (id UInt64, d Dynamic) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
test
$CH_CLIENT -q "drop table test;"
