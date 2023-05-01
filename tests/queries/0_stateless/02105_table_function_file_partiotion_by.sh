#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# See 01658_read_file_to_string_column.sh
user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p "${user_files_path}/"
chmod 777 ${user_files_path}

FILE_PATH="${user_files_path}/test_table_function_file"

function cleanup()
{
    rm -r ${FILE_PATH}
}
trap cleanup EXIT

values="(1, 2, 3), (3, 2, 1), (1, 3, 2)"
${CLICKHOUSE_CLIENT} --query="insert into table function file('${FILE_PATH}/test_{_partition_id}', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') PARTITION BY column3 values ${values}";
echo 'part 1'
${CLICKHOUSE_CLIENT} --query="select * from file('${FILE_PATH}/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')";
echo 'part 2'
${CLICKHOUSE_CLIENT} --query="select * from file('${FILE_PATH}/test_2', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')";
echo 'part 3'
${CLICKHOUSE_CLIENT} --query="select * from file('${FILE_PATH}/test_3', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')";

