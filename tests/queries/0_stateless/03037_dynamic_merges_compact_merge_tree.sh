#!/usr/bin/env bash
# Tags: long, no-tsan, no-asan, no-msan, no-ubsan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh



CH_CLIENT="$CLICKHOUSE_CLIENT --allow_merge_tree_settings --allow_experimental_dynamic_type=1 --compact_parts_max_granules_to_buffer 5"

$CH_CLIENT -q "drop table if exists test;"
$CH_CLIENT -q "create table test (id UInt64, d Dynamic(max_types=2)) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000, index_granularity_bytes=10485760, index_granularity=8192, merge_max_block_size=8192, merge_max_block_size_bytes=10485760;"

$CH_CLIENT -q "system stop merges test"
$CH_CLIENT -q "insert into test select number, number from numbers(100000)"
$CH_CLIENT -q "insert into test select number, 'str_' || toString(number) from numbers(80000)"
$CH_CLIENT -q "insert into test select number, range(number % 10 + 1) from numbers(70000)"
$CH_CLIENT -q "insert into test select number, toDate(number) from numbers(60000)"
$CH_CLIENT -q "insert into test select number, toDateTime(number) from numbers(50000)"
$CH_CLIENT -q "insert into test select number, NULL from numbers(100000)"

$CH_CLIENT -q "select count(), dynamicType(d), isDynamicElementInSharedData(d) from test group by dynamicType(d), isDynamicElementInSharedData(d) order by count(), dynamicType(d);"
$CH_CLIENT -nm -q "system start merges test; optimize table test final;"
echo "------------------"
$CH_CLIENT -q "select count(), dynamicType(d), isDynamicElementInSharedData(d) from test group by dynamicType(d), isDynamicElementInSharedData(d) order by count(), dynamicType(d);"

$CH_CLIENT -q "system stop merges test"
$CH_CLIENT -q "insert into test select number, map(number, number) from numbers(200000)"
echo "------------------"
$CH_CLIENT -q "select count(), dynamicType(d), isDynamicElementInSharedData(d) from test group by dynamicType(d), isDynamicElementInSharedData(d) order by count(), dynamicType(d);"
$CH_CLIENT -nm -q "system start merges test; optimize table test final;"
echo "------------------"
$CH_CLIENT -q "select count(), dynamicType(d), isDynamicElementInSharedData(d) from test group by dynamicType(d), isDynamicElementInSharedData(d) order by count(), dynamicType(d);"

$CH_CLIENT -q "system stop merges test"
$CH_CLIENT -q "insert into test select number, tuple(number, number) from numbers(10000)"
echo "------------------"
$CH_CLIENT -q "select count(), dynamicType(d), isDynamicElementInSharedData(d) from test group by dynamicType(d), isDynamicElementInSharedData(d) order by count(), dynamicType(d);"
$CH_CLIENT -nm -q "system start merges test; optimize table test final;"
echo "------------------"
$CH_CLIENT -q "select count(), dynamicType(d), isDynamicElementInSharedData(d) from test group by dynamicType(d), isDynamicElementInSharedData(d) order by count(), dynamicType(d);"

$CH_CLIENT -q "drop table test;"

