#!/usr/bin/env bash
# Tags: no-backward-compatibility-check

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Data preparation.
# Now we can get the user_files_path by use the table file function for trick. also we can get it by query as:
#  "insert into function file('exist.txt', 'CSV', 'val1 char') values ('aaaa'); select _path from file('exist.txt', 'CSV', 'val1 char')"
user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

for i in {1..20}
do
	echo $i, $i >> ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}.txt
done

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}.txt', 'CSV');"

${CLICKHOUSE_CLIENT} --query "select * from file_log order by k;"

for i in {100..120}
do
	echo $i, $i >> ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}.txt
done

${CLICKHOUSE_CLIENT} --query "select * from file_log order by k;"

# touch does not change file content, no event
touch ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}.txt
${CLICKHOUSE_CLIENT} --query "select * from file_log order by k;"

${CLICKHOUSE_CLIENT} --query "detach table file_log;"
${CLICKHOUSE_CLIENT} --query "attach table file_log;"

# should no records return
${CLICKHOUSE_CLIENT} --query "select * from file_log order by k;"

rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}.txt
