#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"
rm -rf "${WORKING_FOLDER}"
mkdir "${WORKING_FOLDER}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_async_insert"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_async_insert (a String) Engine = MergeTree() ORDER BY ()"
${CLICKHOUSE_CLIENT} --query "SELECT cast(if(number < 65000,'x', randomString(10)) as String) FROM numbers(140000) INTO OUTFILE '${WORKING_FOLDER}/data.clickhouse' FORMAT rowBinary"
${CLICKHOUSE_CLIENT} --query "SET async_insert = 1; set async_insert_max_data_size = 660000; INSERT INTO test_async_insert FROM INFILE '${WORKING_FOLDER}/data.clickhouse' FORMAT rowBinary"
${CLICKHOUSE_CLIENT} --query "SELECT count(*) FROM test_async_insert"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_async_insert" 
