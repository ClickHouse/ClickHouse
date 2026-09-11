#!/usr/bin/env bash
# Tags: long

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p ${CLICKHOUSE_USER_FILES_UNIQUE}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${CLICKHOUSE_USER_FILES_UNIQUE}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*
chmod 777 ${CLICKHOUSE_USER_FILES_UNIQUE}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

for i in {1..10}
do
	${CLICKHOUSE_CLIENT} --query "insert into function file('${CLICKHOUSE_USER_FILES_UNIQUE}/${CLICKHOUSE_TEST_UNIQUE_NAME}/test$i.csv', 'CSV', 'k UInt32, v UInt32') select number, number from numbers(10000);"
done

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt32, v UInt32) engine=FileLog('${CLICKHOUSE_USER_FILES_UNIQUE}/${CLICKHOUSE_TEST_UNIQUE_NAME}/', 'CSV');"

${CLICKHOUSE_CLIENT} --query "select count() from file_log settings stream_like_engine_allow_direct_select=1;"

for i in {11..20}
do
	${CLICKHOUSE_CLIENT} --query "insert into function file('${CLICKHOUSE_USER_FILES_UNIQUE}/${CLICKHOUSE_TEST_UNIQUE_NAME}/test$i.csv', 'CSV', 'k UInt32, v UInt32') select number, number from numbers(10000);"
done

${CLICKHOUSE_CLIENT} --query "select count() from file_log settings stream_like_engine_allow_direct_select=1;"

${CLICKHOUSE_CLIENT} --query "drop table file_log;"

rm -rf ${CLICKHOUSE_USER_FILES_UNIQUE}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
