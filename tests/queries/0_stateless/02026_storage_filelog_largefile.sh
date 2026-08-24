#!/usr/bin/env bash
# Tags: long, no-parallel

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*
chmod 777 ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

for i in {1..10}
do
	${CLICKHOUSE_CLIENT} --query "insert into function file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/test$i.csv', 'CSV', 'k UInt32, v UInt32') select number, number from numbers(10000);"
done

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt32, v UInt32) engine=FileLog('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/', 'CSV');"

${CLICKHOUSE_CLIENT} --query "select count() from file_log settings stream_like_engine_allow_direct_select=1;"

for i in {11..20}
do
	${CLICKHOUSE_CLIENT} --query "insert into function file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/test$i.csv', 'CSV', 'k UInt32, v UInt32') select number, number from numbers(10000);"
done

${CLICKHOUSE_CLIENT} --query "select count() from file_log settings stream_like_engine_allow_direct_select=1;"

${CLICKHOUSE_CLIENT} --query "drop table file_log;"

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
