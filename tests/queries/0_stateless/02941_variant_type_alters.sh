#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --use_variant_as_common_type=1 "

function run()
{
    echo "initial insert"
    $CH_CLIENT -q "insert into test select number, number from numbers(3)"

    echo "alter add column 1"
    $CH_CLIENT -q "alter table test add column v Variant(UInt64, String) settings mutations_sync=1"
    $CH_CLIENT -q "select x, y, v, v.String, v.UInt64 from test order by x"

    echo "insert after alter add column 1"
    $CH_CLIENT -q "insert into test select number, number, number from numbers(3, 3)"
    $CH_CLIENT -q "insert into test select number, number, 'str_' || toString(number) from numbers(6, 3)"
    $CH_CLIENT -q "insert into test select number, number, NULL from numbers(9, 3)"
    $CH_CLIENT -q "insert into test select number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) from numbers(12, 3)"
    $CH_CLIENT -q "select x, y, v, v.String, v.UInt64 from test order by x"

    echo "alter modify column 1"
    $CH_CLIENT -q "alter table test modify column v Variant(UInt64, String, Date) settings mutations_sync=1"
    $CH_CLIENT -q "select x, y, v, v.String, v.UInt64, v.Date from test order by x"

    echo "insert after alter modify column 1"
    $CH_CLIENT -q "insert into test select number, number, toDate(number) from numbers(15, 3)"
    $CH_CLIENT -q "insert into test select number, number, multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, toDate(number), NULL) from numbers(18, 4)"
    $CH_CLIENT -q "select x, y, v, v.String, v.UInt64, v.Date from test order by x"

    echo "alter modify column 2"
    $CH_CLIENT -q "alter table test modify column y Variant(UInt64, String) settings mutations_sync=1"
    $CH_CLIENT -q "select x, y, y.UInt64, y.String, v, v.String, v.UInt64, v.Date from test order by x"

    echo "insert after alter modify column 2"
    $CH_CLIENT -q "insert into test select number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL), NULL from numbers(22, 3)"
    $CH_CLIENT -q "select x, y, y.UInt64, y.String, v, v.String, v.UInt64, v.Date from test order by x"
}

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (x UInt64, y UInt64) engine=Memory"
run
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (x UInt64, y UInt64) engine=MergeTree order by x settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000;"
run
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (x UInt64, y UInt64 ) engine=MergeTree order by x settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
run
$CH_CLIENT -q "drop table test;"
