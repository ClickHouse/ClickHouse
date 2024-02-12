#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --use_variant_as_common_type=1 --allow_experimental_object_type=1"


function create_table()
{
    $CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, String, Float64)) engine=$1;"
}

function drop_table()
{
    $CH_CLIENT -q "drop table if exists test;"
}

function test_order_by
{
    create_table "$1"

    $CH_CLIENT -q  "insert into test values(1, 4);"
    $CH_CLIENT -q  "insert into test values(2, 2);"
    $CH_CLIENT -q  "insert into test values(3, 1);"
    $CH_CLIENT -q  "insert into test values(4, 5);"
    $CH_CLIENT -q  "insert into test values(5, 3);"

    $CH_CLIENT -q  "insert into test values(6, 'b');"
    $CH_CLIENT -q  "insert into test values(7, 'd');"
    $CH_CLIENT -q  "insert into test values(8, 'a');"
    $CH_CLIENT -q  "insert into test values(9, 'c');"
    $CH_CLIENT -q  "insert into test values(10, 'e');"

    $CH_CLIENT -q  "insert into test values(11, 12.0);"
    $CH_CLIENT -q  "insert into test values(12, 13.5);"
    $CH_CLIENT -q  "insert into test values(13, 15.0);"
    $CH_CLIENT -q  "insert into test values(14, 13.0);"
    $CH_CLIENT -q  "insert into test values(15, 15.20);"
    $CH_CLIENT -q  "insert into test values(16, 11.0);"
    $CH_CLIENT -q  "insert into test values(17, 15.25);"
    $CH_CLIENT -q  "insert into test values(18, 14.0);"

    #$CH_CLIENT -nmq "select id, v, variantType(v) from test order by v;" # Does not sort

    $CH_CLIENT -nmq "select id, v, variantType(v) from test order by v.UInt64;"

    $CH_CLIENT -nmq "select id, v, variantType(v) from test order by v.String;"

    $CH_CLIENT -nmq "select id, v, variantType(v) from test order by v.Float64;"

    $CH_CLIENT -nmq "select id, v, variantType(v) from test order by v.UInt64, v.String, v.Float64;"

    $CH_CLIENT -nmq "select id, v, variantType(v) from test order by v.UInt64 desc, v.String desc, v.Float64 desc;"
}

function test_group_by()
{
    create_table "$1"

    $CH_CLIENT -q  "insert into test values(1, 1);"
    $CH_CLIENT -q  "insert into test values(2, 1);"
    $CH_CLIENT -q  "insert into test values(3, 1);"
    $CH_CLIENT -q  "insert into test values(4, 2);"
    $CH_CLIENT -q  "insert into test values(5, 2);"

    $CH_CLIENT -q  "insert into test values(6, 'a');"
    $CH_CLIENT -q  "insert into test values(7, 'a');"
    $CH_CLIENT -q  "insert into test values(8, 'b');"
    $CH_CLIENT -q  "insert into test values(9, 'b');"
    $CH_CLIENT -q  "insert into test values(10, 'b');"

    $CH_CLIENT -q  "insert into test values(11, '2');"

    $CH_CLIENT -q  "insert into test values(12, 12.0);"
    $CH_CLIENT -q  "insert into test values(13, 12.0);"
    $CH_CLIENT -q  "insert into test values(14, 13.25);"
    $CH_CLIENT -q  "insert into test values(15, 13.25);"
    $CH_CLIENT -q  "insert into test values(16, 13.25);"
    $CH_CLIENT -q  "insert into test values(17, 14.5);"
    $CH_CLIENT -q  "insert into test values(18, 14.5);"
    $CH_CLIENT -q  "insert into test values(19, 15.025);"
    $CH_CLIENT -q  "insert into test values(20, 15.025);"

    $CH_CLIENT -nmq "select v, variantType(v), count(*) from test.test group by v;"

    #$CH_CLIENT -nmq "select id, v, variantType(v), max(id) over (partition by v) as max_id from test;" # bucketing is wrong
}

engines=("Memory" "MergeTree order by id settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000" "MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1")

for engine in "${engines[@]}"; do

    drop_table

    echo "$engine"

    test_order_by "$engine"

    drop_table

    test_group_by "$engine"

 done
