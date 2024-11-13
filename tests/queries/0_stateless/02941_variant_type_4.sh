#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --allow_suspicious_variant_types=1"

function test6_insert()
{
    echo "test6 insert"
    $CH_CLIENT -q "insert into test with 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))' as type select number, multiIf(number % 6 == 0, CAST(NULL, type), number % 6 == 1, CAST(('str_' || toString(number))::Variant(String), type), number % 6 == 2, CAST(number, type), number % 6 == 3, CAST(('lc_str_' || toString(number))::LowCardinality(String), type), number % 6 == 4, CAST(tuple(number, number + 1)::Tuple(a UInt32, b UInt32), type), CAST(range(number % 20 + 1)::Array(UInt64), type)) as res from numbers(1200000);"
}

function test6_select()
{
    echo "test6 select"
    $CH_CLIENT -mq "select v from test format Null;
    select count() from test where isNotNull(v);
    select v.String from test format Null;
    select count() from test where isNotNull(v.String);
    select v.UInt64 from test format Null;
    select count() from test where isNotNull(v.UInt64);
    select v.\`LowCardinality(String)\` from test format Null;
    select count() from test where isNotNull(v.\`LowCardinality(String)\`);
    select v.\`Tuple(a UInt32, b UInt32)\` from test format Null;
    select v.\`Tuple(a UInt32, b UInt32)\`.a from test format Null;
    select v.\`Tuple(a UInt32, b UInt32)\`.b from test format Null;
    select v.\`Array(UInt64)\` from test format Null;
    select count() from test where not empty(v.\`Array(UInt64)\`);
    select v.\`Array(UInt64)\`.size0 from test format Null;"
    echo "-----------------------------------------------------------------------------------------------------------"
}

function run()
{
    test6_insert
    test6_select
    if [ $1 == 1 ]; then
        $CH_CLIENT -q "optimize table test final;"
        test6_select
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
