#!/usr/bin/env bash

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
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/', 'CSV');"

${CLICKHOUSE_CLIENT} --query "select *, _filename, _offset from file_log order by  _filename, _offset settings stream_like_engine_allow_direct_select=1;"

cp ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/b.txt

${CLICKHOUSE_CLIENT} --query "select *, _filename, _offset from file_log order by  _filename, _offset settings stream_like_engine_allow_direct_select=1;"

for i in {100..120}
do
	echo $i, $i >> ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt
done

# touch does not change file content, no event
touch ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt

cp ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/c.txt
cp ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/d.txt
cp ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/e.txt

rm ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/d.txt

${CLICKHOUSE_CLIENT} --query "select *, _filename, _offset from file_log order by  _filename, _offset settings stream_like_engine_allow_direct_select=1;"

${CLICKHOUSE_CLIENT} --query "detach table file_log;"
${CLICKHOUSE_CLIENT} --query "attach table file_log;"

# should no records return
${CLICKHOUSE_CLIENT} --query "select *, _filename, _offset from file_log order by  _filename, _offset settings stream_like_engine_allow_direct_select=1;"

truncate ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.txt --size 0

# exception happend
${CLICKHOUSE_CLIENT} --query "select * from file_log order by k settings stream_like_engine_allow_direct_select=1;" 2>&1 | grep -q "Code: 33" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "drop table file_log;"

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
