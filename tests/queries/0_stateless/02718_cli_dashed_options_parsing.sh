#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

file_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
file_name_1=${file_name}_1
file_name_2=${file_name}_2
file_name_3=${file_name}_3

#################
echo "Test 1: Check that you can specify options with a dashes, not an underscores"

[[ -e $file_name_1 ]] && rm $file_name_1
[[ -e $file_name_2 ]] && rm $file_name_2
[[ -e $file_name_3 ]] && rm $file_name_3

echo "Test 1.1: Check option from config - server_logs_file"

$CLICKHOUSE_LOCAL --log-level=debug --server-logs-file=$file_name_1 -q "SELECT 1;" 2> /dev/null
ls | grep -q $file_name_1
echo $?
$CLICKHOUSE_LOCAL --log-level=debug --server-logs-file $file_name_2 -q "SELECT 1;" 2> /dev/null
ls | grep -q $file_name_2
echo $?
$CLICKHOUSE_LOCAL --log-level=debug --server_logs_file $file_name_3 -q "SELECT 1;" 2> /dev/null
ls | grep -q $file_name_3
echo $?

echo "Test 1.1: Check some option from Settings.h - allow_deprecated_syntax_for_merge_tree"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test";
$CLICKHOUSE_CLIENT --allow-deprecated-syntax-for-merge-tree=1 --query="CREATE TABLE test (d Date, s String) ENGINE = MergeTree(d, s, 8192)";
$CLICKHOUSE_CLIENT --query="DROP TABLE test";
echo $?

#################
echo "Test 2: check that unicode dash processed correctly"
$CLICKHOUSE_LOCAL —query "SELECT 1";

rm $file_name_1
rm $file_name_2
rm $file_name_3
