#!/usr/bin/env bash
# Tags: long, no-debug

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --use_variant_as_common_type=1 --allow_suspicious_variant_types=1"


function test1_insert()
{
    echo "test1 insert"
    $CH_CLIENT -q "insert into test select number, number::Variant(UInt64)::Variant(UInt64, Array(UInt64)) from numbers(10) settings max_block_size=3"
    $CH_CLIENT -q "insert into test select number, if(number % 2, NULL, number)::Variant(UInt64)::Variant(UInt64, String, Array(UInt64)) as res from numbers(10, 10) settings max_block_size=3"
    $CH_CLIENT -q "insert into test select number, if(number % 2, NULL, 'str_' || toString(number))::Variant(String)::Variant(UInt64, String, Array(UInt64)) as res from numbers(20, 10) settings max_block_size=3"
    $CH_CLIENT -q "insert into test select number, if(number < 35, if(number % 2, NULL, number)::Variant(UInt64)::Variant(UInt64, String, Array(UInt64)), if(number % 2, NULL, 'str_' || toString(number))::Variant(String)::Variant(UInt64, String, Array(UInt64))) from numbers(30, 10) settings max_block_size=3"
}

function test1_select()
{
    echo "test1 select"
    $CH_CLIENT -q "select v, v.String, v.UInt64 from test order by id;"
    echo "-----------------------------------------------------------------------------------------------------------"
}

function test2_insert()
{
    echo "test2 insert"
    $CH_CLIENT -q "insert into test select number, number::Variant(UInt64)::Variant(UInt64, Array(UInt64)) from numbers(200000) settings max_insert_block_size = 10000, min_insert_block_size_rows=10000"
    $CH_CLIENT -q "insert into test select number, if(number % 2, NULL, number)::Variant(UInt64)::Variant(UInt64, String, Array(UInt64)) as res from numbers(200000, 200000) settings max_insert_block_size = 10000, min_insert_block_size_rows=10000"
    $CH_CLIENT -q "insert into test select number, if(number % 2, NULL, 'str_' || toString(number))::Variant(String)::Variant(UInt64, String, Array(UInt64)) as res from numbers(400000, 200000) settings max_insert_block_size = 10000, min_insert_block_size_rows=10000"
    $CH_CLIENT -q "insert into test select number, if(number < 3500000, if(number % 2, NULL, number)::Variant(UInt64)::Variant(UInt64, String, Array(UInt64)), if(number % 2, NULL, 'str_' || toString(number))::Variant(String)::Variant(UInt64, String, Array(UInt64))) from numbers(600000, 200000) settings max_insert_block_size = 10000, min_insert_block_size_rows=10000"
}

function test2_select()
{
    echo "test2 select"
    $CH_CLIENT -q "select v, v.String, v.UInt64 from test format Null;"
    $CH_CLIENT -q "select v from test format Null;"
    $CH_CLIENT -q "select count() from test where isNotNull(v);"
    $CH_CLIENT -q "select v.String from test format Null;"
    $CH_CLIENT -q "select count() from test where isNotNull(v.String);"
    $CH_CLIENT -q "select v.UInt64 from test format Null;"
    $CH_CLIENT -q "select count() from test where isNotNull(v.UInt64);"
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
}

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, String, Array(UInt64))) engine=Memory;"
run 0
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, String, Array(UInt64))) engine=MergeTree order by id settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000, index_granularity = 8192, index_granularity_bytes = '10Mi';"
run 1
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, String, Array(UInt64))) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, index_granularity = 8192, index_granularity_bytes = '10Mi';"
run 1
$CH_CLIENT -q "drop table test;"
