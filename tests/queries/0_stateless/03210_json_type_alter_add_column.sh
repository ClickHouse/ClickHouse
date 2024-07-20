#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 --allow_experimental_variant_type=1 --use_variant_as_common_type=1"

function run()
{
    echo "initial insert"
    $CH_CLIENT -q "insert into test select number from numbers(3)"

    echo "alter add column 1"
    $CH_CLIENT -q "alter table test add column json JSON settings mutations_sync=1"
    $CH_CLIENT -q "select count(), arrayJoin(JSONAllPaths(json)) as path from test group by path order by count() desc, path"
    $CH_CLIENT -q "select x, json, json.a.b, json.^a, json.b.c.:Int64, json.c.d from test order by x"

    echo "insert after alter add column"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('a.b', number::UInt32)) from numbers(3, 3)"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('b.c', number::UInt32)) from numbers(6, 3)"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('c.d', number::UInt32)) from numbers(9, 3)"
    $CH_CLIENT -q "insert into test select number, '{}' from numbers(12, 3)"
    $CH_CLIENT -q "select count(), arrayJoin(JSONAllPaths(json)) as path from test group by path order by count() desc, path"
    $CH_CLIENT -q "select x, json, json.a.b, json.^a, json.b.c.:Int64, json.c.d from test order by x"
}

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (x UInt64) engine=Memory"
run
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (x UInt64) engine=MergeTree order by x settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000;"
run
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (x UInt64) engine=MergeTree order by x settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
run
$CH_CLIENT -q "drop table test;"

