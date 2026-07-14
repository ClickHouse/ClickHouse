#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 --allow_experimental_variant_type=1 --use_variant_as_common_type=1"

function run()
{
    echo "initial insert"
    $CH_CLIENT -q "insert into test select number, number from numbers(3)"

    echo "alter add column"
    $CH_CLIENT -q "alter table test add column d Dynamic(max_types=2) settings mutations_sync=1"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "insert after alter add column"
    $CH_CLIENT -q "insert into test select number, number, number from numbers(3, 3)"
    $CH_CLIENT -q "insert into test select number, number, 'str_' || toString(number) from numbers(6, 3)"
    $CH_CLIENT -q "insert into test select number, number, NULL from numbers(9, 3)"
    $CH_CLIENT -q "insert into test select number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) from numbers(12, 3)"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.Date, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "check table"
    $CH_CLIENT -q "check table test"
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
