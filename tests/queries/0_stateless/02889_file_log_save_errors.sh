#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"
${CLICKHOUSE_CLIENT} --query "drop table if exists log_errors;"

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*

for i in {0..9}
do
	echo "{\"key\" : $i, \"value\" : $i}" >> ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.jsonl
	echo "Error $i" >> ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.jsonl
done

for i in {10..19}
do
	echo "{\"key\" : $i, \"value\" : $i}" >> ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/b.jsonl
	echo "Error $i" >> ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/b.jsonl
done

${CLICKHOUSE_CLIENT} --query "create table file_log(key UInt8, value UInt8) engine=FileLog('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/', 'JSONEachRow') settings handle_error_mode='stream';"
${CLICKHOUSE_CLIENT} --query "create Materialized View log_errors engine=MergeTree order by tuple() as select _error as error, _raw_record as record, _filename as file from file_log where not isNull(_error);"

function count()
{
	COUNT=$(${CLICKHOUSE_CLIENT} --query "select count() from log_errors;")
	echo $COUNT
}

while true; do
	[[ $(count) == 20 ]] && break
	sleep 1
done

${CLICKHOUSE_CLIENT} --query "select * from log_errors order by file, record;"
${CLICKHOUSE_CLIENT} --query "drop table file_log;"
${CLICKHOUSE_CLIENT} --query "drop table log_errors;"

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
