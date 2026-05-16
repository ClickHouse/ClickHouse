#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"
${CLICKHOUSE_CLIENT} --query "drop table if exists log_errors;"

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*

for i in {0..2}
do
	echo "{\"key\" : $i, \"value\" : $i}" >> ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.jsonl
	echo "Error $i" >> ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.jsonl
done

for i in {3..5}
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

# Wait for all 6 error records with a bounded timeout.
# Reduced from 20 to 6 errors (3 per file) so the FileLog background thread
# can finish quickly even under TSAN/MSAN overhead. Normal runtime is ~14s;
# the 300s ceiling provides headroom for the slowest random settings the
# flaky check explores while still preventing an unbounded hang.
TIMEOUT=300
START=$EPOCHSECONDS
while [[ $(count) != 6 ]]; do
	if ((EPOCHSECONDS - START > TIMEOUT)); then
		echo "Timeout (${TIMEOUT}s) waiting for 6 error records in log_errors. Got $(count)."
		${CLICKHOUSE_CLIENT} --query "drop table file_log;"
		${CLICKHOUSE_CLIENT} --query "drop table log_errors;"
		rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
		exit 1
	fi
	sleep 1
done

${CLICKHOUSE_CLIENT} --query "select * from log_errors order by file, record;"
${CLICKHOUSE_CLIENT} --query "drop table file_log;"
${CLICKHOUSE_CLIENT} --query "drop table log_errors;"

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
