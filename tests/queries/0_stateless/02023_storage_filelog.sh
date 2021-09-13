#!/usr/bin/env bash
set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Data preparation.
# Now we can get the user_files_path by use the table file function for trick. also we can get it by query as:
#  "insert into function file('exist.txt', 'CSV', 'val1 char') values ('aaaa'); select _path from file('exist.txt', 'CSV', 'val1 char')"
user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p ${user_files_path}/logs/
echo  1, 1 >> ${user_files_path}/logs/a.txt
echo  2, 2 >> ${user_files_path}/logs/a.txt
echo  3, 3 >> ${user_files_path}/logs/a.txt
echo  4, 4 >> ${user_files_path}/logs/a.txt
echo  5, 5 >> ${user_files_path}/logs/a.txt
echo  6, 6 >> ${user_files_path}/logs/a.txt
echo  7, 7 >> ${user_files_path}/logs/a.txt
echo  8, 8 >> ${user_files_path}/logs/a.txt
echo  9, 9 >> ${user_files_path}/logs/a.txt
echo  10, 10 >> ${user_files_path}/logs/a.txt

### 1st TEST in CLIENT mode.
${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('logs', 'CSV');"

${CLICKHOUSE_CLIENT} --query "select * from file_log order by k;"

echo  100, 100 >> ${user_files_path}/logs/a.txt

${CLICKHOUSE_CLIENT} --query "select * from file_log order by k;"

cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/b.txt

${CLICKHOUSE_CLIENT} --query "select * from file_log order by k;"

${CLICKHOUSE_CLIENT} --query "drop table if exists mv;"
${CLICKHOUSE_CLIENT} --query "create Materialized View mv engine=MergeTree order by k as select * from file_log;"

cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/c.txt
cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/d.txt

sleep 10

${CLICKHOUSE_CLIENT} --query "select * from mv order by k;"

echo  111, 111 >> ${user_files_path}/logs/a.txt

sleep 10

${CLICKHOUSE_CLIENT} --query "select * from mv order by k;"

${CLICKHOUSE_CLIENT} --query "drop table file_log;"
${CLICKHOUSE_CLIENT} --query "drop table mv;"

rm -rf ${user_files_path}/logs
