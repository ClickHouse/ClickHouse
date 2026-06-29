#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --allow_suspicious_variant_types=1"

function test1_insert()
{
    echo "test1 insert"
    $CH_CLIENT -mq "insert into test select number, NULL from numbers(3);
insert into test select number + 3, number from numbers(3);
insert into test select number + 6, ('str_' || toString(number))::Variant(String) from numbers(3);
insert into test select number + 9, ('lc_str_' || toString(number))::LowCardinality(String) from numbers(3);
insert into test select number + 12, tuple(number, number + 1)::Tuple(a UInt32, b UInt32) from numbers(3);
insert into test select number + 15, range(number + 1)::Array(UInt64) from numbers(3);"
}

function test1_select()
{
    echo "test1 select"
    $CH_CLIENT -mq "select v from test order by id;
select v.String from test order by id;
select v.UInt64 from test order by id;
select v.\`LowCardinality(String)\` from test order by id;
select v.\`Tuple(a UInt32, b UInt32)\` from test order by id;
select v.\`Tuple(a UInt32, b UInt32)\`.a from test order by id;
select v.\`Tuple(a UInt32, b UInt32)\`.b from test order by id;
select v.\`Array(UInt64)\` from test order by id;
select v.\`Array(UInt64)\`.size0 from test order by id;"
    echo "-----------------------------------------------------------------------------------------------------------"
}

function test2_insert()
{
    echo "test2 insert"
    $CH_CLIENT -mq "insert into test select number, NULL from numbers(3);
insert into test select number + 3, number % 2 ? NULL : number from numbers(3);
insert into test select number + 6, number % 2 ? NULL : ('str_' || toString(number))::Variant(String) from numbers(3);
insert into test select number + 9, number % 2 ? CAST(NULL, 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') : CAST(('lc_str_' || toString(number))::LowCardinality(String), 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') from numbers(3);
insert into test select number + 12, number % 2 ? CAST(NULL, 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') : CAST(tuple(number, number + 1)::Tuple(a UInt32, b UInt32), 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') from numbers(3);
insert into test select number + 15, number % 2 ? CAST(NULL, 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') : CAST(range(number + 1)::Array(UInt64), 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') from numbers(3);"
}

function test2_select()
{
    echo "test2 select"
    $CH_CLIENT -mq "select v from test order by id;
select v.String from test order by id;
select v.UInt64 from test order by id;
select v.\`LowCardinality(String)\` from test order by id;
select v.\`Tuple(a UInt32, b UInt32)\` from test order by id;
select v.\`Tuple(a UInt32, b UInt32)\`.a from test order by id;
select v.\`Tuple(a UInt32, b UInt32)\`.b from test order by id;
select v.\`Array(UInt64)\` from test order by id;
select v.\`Array(UInt64)\`.size0 from test order by id;"
    echo "-----------------------------------------------------------------------------------------------------------"
}

function test3_insert()
{
    echo "test3 insert"
    $CH_CLIENT -q "insert into test with 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))' as type select number, multiIf(number % 6 == 0, CAST(NULL, type), number % 6 == 1, CAST(('str_' || toString(number))::Variant(String), type), number % 6 == 2, CAST(number, type), number % 6 == 3, CAST(('lc_str_' || toString(number))::LowCardinality(String), type), number % 6 == 4, CAST(tuple(number, number + 1)::Tuple(a UInt32, b UInt32), type), CAST(range(number + 1)::Array(UInt64), type)) as res from numbers(18);"
}

function test3_select()
{
    echo "test3 select"
    $CH_CLIENT -mq "select v from test order by id;
select v.String from test order by id;
select v.UInt64 from test order by id;
select v.\`LowCardinality(String)\` from test order by id;
select v.\`Tuple(a UInt32, b UInt32)\` from test order by id;
select v.\`Tuple(a UInt32, b UInt32)\`.a from test order by id;
select v.\`Tuple(a UInt32, b UInt32)\`.b from test order by id;
select v.\`Array(UInt64)\` from test order by id;
select v.\`Array(UInt64)\`.size0 from test order by id;"
    echo "-----------------------------------------------------------------------------------------------------------"
}

function run()
{
    test1_insert
    test1_select
    if [ $1 == 1 ]; then
        $CH_CLIENT -q "optimize table test final;"
        test1_select
    fi
    $CH_CLIENT -q "truncate table test;"
    test2_insert
    test2_select
    if [ $1 == 1 ]; then
        $CH_CLIENT -q "optimize table test final;"
        test2_select
    fi
    $CH_CLIENT -q "truncate table test;"
    test3_insert
    test3_select
    if [ $1 == 1 ]; then
        $CH_CLIENT -q "optimize table test final;"
        test3_select
    fi
    $CH_CLIENT -q "truncate table test;"
}

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (id UInt64, v Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))) engine=Memory;"
run 0
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (id UInt64, v Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))) engine=MergeTree order by id settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000, index_granularity_bytes=10485760, index_granularity=8192;"
run 1
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (id UInt64, v Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, index_granularity_bytes=10485760, index_granularity=8192;"
run 1
$CH_CLIENT -q "drop table test;"
