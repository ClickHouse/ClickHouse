#!/usr/bin/env bash
# Tags: no-fasttest, long, no-debug, no-tsan, no-asan, no-msan, no-ubsan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_json_type=1"


function test()
{
    echo "test"
    $CH_CLIENT -q "system stop merges test"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('a', number)) from numbers(100000)"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('b', arrayMap(x -> map('c', x), range(number % 5 + 1)))) from numbers(100000)"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('b', arrayMap(x -> map('d', x), range(number % 5 + 1)))) from numbers(50000)"

    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(arrayJoin(json.b[]))) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(arrayJoin(json.b[]))) as path from test group by path order by count() desc, path"
    $CH_CLIENT -nm -q "system start merges test; optimize table test final;"
    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(arrayJoin(json.b[]))) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(arrayJoin(json.b[]))) as path from test group by path order by count() desc, path"

    $CH_CLIENT -q "insert into test select number, toJSONString(map('b', arrayMap(x -> map('e', x), range(number % 5 + 1)))) from numbers(50000)"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('b', arrayMap(x -> map('f', x), range(number % 5 + 1)))) from numbers(200000)"

    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(arrayJoin(json.b[]))) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(arrayJoin(json.b[]))) as path from test group by path order by count() desc, path"
    $CH_CLIENT -nm -q "system start merges test; optimize table test final;"
    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(arrayJoin(json.b[]))) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(arrayJoin(json.b[]))) as path from test group by path order by count() desc, path"
}

$CH_CLIENT -q "drop table if exists test;"

echo "MergeTree compact + horizontal merge"
$CH_CLIENT -q "create table test (id UInt64, json JSON(max_dynamic_paths=4)) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide + horizontal merge"
$CH_CLIENT -q "create table test (id UInt64, json JSON(max_dynamic_paths=4)) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact + vertical merge"
$CH_CLIENT -q "create table test (id UInt64, json JSON(max_dynamic_paths=4)) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide + vertical merge"
$CH_CLIENT -q "create table test (id UInt64, json JSON(max_dynamic_paths=4)) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;"
test
$CH_CLIENT -q "drop table test;"
