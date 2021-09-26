#!/usr/bin/env bash
# Tags: no-parallel

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Data preparation.
# Now we can get the user_files_path by use the table file function for trick. also we can get it by query as:
#  "insert into function file('exist.txt', 'CSV', 'val1 char') values ('aaaa'); select _path from file('exist.txt', 'CSV', 'val1 char')"
user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p ${user_files_path}/logs/

rm -rf ${user_files_path}/logs/*

for i in {1..20}
do
	echo $i, $i >> ${user_files_path}/logs/a.txt
done

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('logs', 'CSV');"

${CLICKHOUSE_CLIENT} --query "select *, _file_name, _offset from file_log order by  _file_name, _offset;"

cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/b.txt

${CLICKHOUSE_CLIENT} --query "select *, _file_name, _offset from file_log order by  _file_name, _offset;"

for i in {100..120}
do
	echo $i, $i >> ${user_files_path}/logs/a.txt
done

# touch does not change file content, no event
touch ${user_files_path}/logs/a.txt

cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/c.txt
cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/d.txt
cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/e.txt

rm ${user_files_path}/logs/d.txt

${CLICKHOUSE_CLIENT} --query "select *, _file_name, _offset from file_log order by  _file_name, _offset;"

${CLICKHOUSE_CLIENT} --query "detach table file_log;"
${CLICKHOUSE_CLIENT} --query "attach table file_log;"

# should no records return
${CLICKHOUSE_CLIENT} --query "select *, _file_name, _offset from file_log order by  _file_name, _offset;"

truncate ${user_files_path}/logs/a.txt --size 0

# exception happend
${CLICKHOUSE_CLIENT} --query "select * from file_log order by k;" 2>&1 | grep -q "Code: 626" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "drop table file_log;"

rm -rf ${user_files_path}/logs
