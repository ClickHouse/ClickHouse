#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_PATH="${USER_FILES_PATH}/test_table_function_file"

function cleanup()
{
    rm -r ${FILE_PATH}
}
trap cleanup EXIT

values="(1, 2, 3), (3, 2, 1), (1, 3, 2)"
${CLICKHOUSE_CLIENT} --query="insert into table function file('${FILE_PATH}/{_partition_id}/test_{_partition_id}', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') PARTITION BY column3 values ${values}";
echo 'part 1'
${CLICKHOUSE_CLIENT} --query="select * from file('${FILE_PATH}/1/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')";
echo 'part 2'
${CLICKHOUSE_CLIENT} --query="select * from file('${FILE_PATH}/2/test_2', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')";
echo 'part 3'
${CLICKHOUSE_CLIENT} --query="select * from file('${FILE_PATH}/3/test_3', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')";
