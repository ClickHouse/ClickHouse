#!/usr/bin/env bash
# Tags: long, no-parallel

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

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('${user_files_path}/logs/', 'CSV');"

${CLICKHOUSE_CLIENT} --query "drop table if exists mv;"
${CLICKHOUSE_CLIENT} --query "create Materialized View mv engine=MergeTree order by k as select * from file_log;"

for i in {1..20}
do
	echo $i, $i >> ${user_files_path}/logs/a.txt
done

for i in {1..200}
do
	sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "select * from mv order by k;"

cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/b.txt

# touch does not change file content, no event
touch ${user_files_path}/logs/a.txt

cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/c.txt
cp ${user_files_path}/logs/a.txt ${user_files_path}/logs/d.txt

for i in {100..120}
do
	echo $i, $i >> ${user_files_path}/logs/d.txt
done

for _ in {1..300}
do
	sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "select * from mv order by k;"

${CLICKHOUSE_CLIENT} --query "drop table mv;"
${CLICKHOUSE_CLIENT} --query "drop table file_log;"

rm -rf ${user_files_path}/logs
