#!/usr/bin/env bash
# Tags: long, no-parallel
# Tag no-parallel: FileLog -> MV streaming latency depends on `BackgroundSchedulePool`
# scheduling; under heavy parallel load the detection wait can drift past its timeout even
# with a bounded backoff. Same precedent as `02968_file_log_multiple_read.sh`.

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*

for i in {1..20}
do
	echo $i, $i >> ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt
done

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"
# poll_directory_watch_events_backoff_max bounds the watcher/reader idle backoff to ~1s
# (default 32s), so new files are detected quickly on slow lanes (e.g. WasmEdge+MSan).
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/', 'CSV') settings poll_directory_watch_events_backoff_max = 1000;"

${CLICKHOUSE_CLIENT} --query "drop table if exists mv;"
${CLICKHOUSE_CLIENT} --query "create Materialized View mv engine=MergeTree order by k as select * from file_log;"

function count()
{
	${CLICKHOUSE_CLIENT} --query "select count() from mv;"
}

function wait_for_count()
{
	local target="$1"
	local timeout=120
	local start=$EPOCHSECONDS
	while [[ $(count) != "$target" ]]; do
		if ((EPOCHSECONDS - start > timeout)); then
			echo "Timeout (${timeout}s) waiting for count() == ${target}, got $(count)."
			exit 1
		fi
		sleep 1
	done
}

wait_for_count 20

${CLICKHOUSE_CLIENT} --query "select * from mv order by k;"

cp ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt

# touch does not change file content, no event
touch ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt

cp ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/c.txt
cp ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/d.txt

for i in {100..120}
do
	echo $i, $i >> ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/d.txt
done

wait_for_count 101

${CLICKHOUSE_CLIENT} --query "select * from mv order by k;"

${CLICKHOUSE_CLIENT} --query "drop table mv;"
${CLICKHOUSE_CLIENT} --query "drop table file_log;"

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
