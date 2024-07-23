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
    $CH_CLIENT -q "insert into test select number, toJSONString(map('b', number)) from numbers(90000)"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('c', number)) from numbers(80000)"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('d', number)) from numbers(70000)"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('e', number)) from numbers(60000)"
    $CH_CLIENT -q "insert into test select number, '{}' from numbers(100000)"

    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(json)) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(json)) as path from test group by path order by count() desc, path"
    $CH_CLIENT -nm -q "system start merges test; optimize table test final;"
    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(json)) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(json)) as path from test group by path order by count() desc, path"

    $CH_CLIENT -q "system stop merges test"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('f', number)) from numbers(200000)"
    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(json)) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(json)) as path from test group by path order by count() desc, path"
    $CH_CLIENT -nm -q "system start merges test; optimize table test final;"
    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(json)) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(json)) as path from test group by path order by count() desc, path"

    $CH_CLIENT -q "system stop merges test"
    $CH_CLIENT -q "insert into test select number, toJSONString(map('g', number)) from numbers(10000)"
    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(json)) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(json)) as path from test group by path order by count() desc, path"
    $CH_CLIENT -nm -q "system start merges test; optimize table test final;"
    echo "Dynamic paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONDynamicPaths(json)) as path from test group by path order by count() desc, path"
    echo "Shared data paths"
    $CH_CLIENT -q "select count(), arrayJoin(JSONSharedDataPaths(json)) as path from test group by path order by count() desc, path"
}

$CH_CLIENT -q "drop table if exists test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (id UInt64, json JSON(max_dynamic_paths=3)) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1, index_granularity_bytes=10485760, index_granularity=8192, merge_max_block_size=8192, merge_max_block_size_bytes=10485760;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (id UInt64, json JSON(max_dynamic_paths=3)) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1, index_granularity_bytes=10485760, index_granularity=8192, merge_max_block_size=8192, merge_max_block_size_bytes=10485760;"
test
$CH_CLIENT -q "drop table test;"

