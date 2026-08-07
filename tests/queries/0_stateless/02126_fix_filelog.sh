#!/usr/bin/env bash

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*

${CLICKHOUSE_CLIENT} --query "drop table if exists file_log;"

${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('/tmp/aaa.csv', 'CSV');" 2>&1 | grep -q "Code: 36" && echo 'OK' || echo 'FAIL';
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('/tmp/aaa.csv', 'CSV');" 2>&1 | grep -q "Code: 36" && echo 'OK' || echo 'FAIL';
${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('/tmp/aaa.csv', 'CSV');" 2>&1 | grep -q "Code: 36" && echo 'OK' || echo 'FAIL';

${CLICKHOUSE_CLIENT} --query "create table file_log(k UInt8, v UInt8) engine=FileLog('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/', 'CSV');"

${CLICKHOUSE_CLIENT} --query "drop table file_log;"

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
