#!/usr/bin/env bash

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Data preparation.
# Now we can get the user_files_path by use the table file function for trick. also we can get it by query as:
#  "insert into function file('exist.txt', 'CSV', 'val1 char') values ('aaaa'); select _path from file('exist.txt', 'CSV', 'val1 char')"
user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"

${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('/tmp/aaa.csv', 'CSV');" 2>&1 | grep -q "Code: 36" && echo 'OK' || echo 'FAIL';
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('/tmp/aaa.csv', 'CSV');" 2>&1 | grep -q "Code: 36" && echo 'OK' || echo 'FAIL';
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('/tmp/aaa.csv', 'CSV');" 2>&1 | grep -q "Code: 36" && echo 'OK' || echo 'FAIL';

${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/', 'CSV');"

${CLICKHOUSE_CLIENT} --query "drop table file_log;"

rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
