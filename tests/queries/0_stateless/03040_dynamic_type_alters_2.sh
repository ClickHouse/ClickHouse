#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 --allow_experimental_variant_type=1 --use_variant_as_common_type=1"

function run()
{
    echo "initial insert"
    $CH_CLIENT -q "insert into test select number, number from numbers(3)"

    echo "alter add column"
    $CH_CLIENT -q "alter table test add column d Dynamic settings mutations_sync=1"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "insert after alter add column 1"
    $CH_CLIENT -q "insert into test select number, number, number from numbers(3, 3)"
    $CH_CLIENT -q "insert into test select number, number, 'str_' || toString(number) from numbers(6, 3)"
    $CH_CLIENT -q "insert into test select number, number, NULL from numbers(9, 3)"
    $CH_CLIENT -q "insert into test select number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) from numbers(12, 3)"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.Date, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "alter rename column 1"
    $CH_CLIENT -q "alter table test rename column d to d1 settings mutations_sync=1"
    $CH_CLIENT -q "select count(), dynamicType(d1) from test group by dynamicType(d1) order by count(), dynamicType(d1)"
    $CH_CLIENT -q "select x, y, d1, d1.String, d1.UInt64, d1.Date, d1.\`Tuple(a UInt64)\`.a from test order by x"

    echo "insert nested dynamic"
    $CH_CLIENT -q "insert into test select number, number, [number % 2 ? number : 'str_' || toString(number)]::Array(Dynamic) from numbers(15, 3)"
    $CH_CLIENT -q "select count(), dynamicType(d1) from test group by dynamicType(d1) order by count(), dynamicType(d1)"
    $CH_CLIENT -q "select x, y, d1, d1.String, d1.UInt64, d1.Date, d1.\`Tuple(a UInt64)\`.a, d1.\`Array(Dynamic)\`.UInt64, d1.\`Array(Dynamic)\`.String, d1.\`Array(Dynamic)\`.Date from test order by x"

    echo "alter rename column 2"
    $CH_CLIENT -q "alter table test rename column d1 to d2 settings mutations_sync=1"
    $CH_CLIENT -q "select count(), dynamicType(d2) from test group by dynamicType(d2) order by count(), dynamicType(d2)"
    $CH_CLIENT -q "select x, y, d2, d2.String, d2.UInt64, d2.Date, d2.\`Tuple(a UInt64)\`.a, d2.\`Array(Dynamic)\`.UInt64, d2.\`Array(Dynamic)\`.String, d2.\`Array(Dynamic)\`.Date, from test order by x"
}

$CH_CLIENT -q "drop table if exists test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (x UInt64, y UInt64) engine=MergeTree order by x settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000;"
run
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (x UInt64, y UInt64 ) engine=MergeTree order by x settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
run
$CH_CLIENT -q "drop table test;"

