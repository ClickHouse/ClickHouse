#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

[ -e "${CLICKHOUSE_TMP}"/test_02270_1.csv ] && rm "${CLICKHOUSE_TMP}"/test_02270_1.csv
[ -e "${CLICKHOUSE_TMP}"/test_02270_2.csv ] && rm "${CLICKHOUSE_TMP}"/test_02270_2.csv

echo "Hello,World" > "${CLICKHOUSE_TMP}"/test_02270_1.csv
echo "Error" > "${CLICKHOUSE_TMP}"/test_02270_2.csv

${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${CLICKHOUSE_TMP}/test_02270*.csv', CSV, 'a String, b String')" 2>&1 | grep -o "test_02270_2.csv"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${CLICKHOUSE_TMP}/test_02270*.csv', CSV, 'a String, b String')" --input_format_parallel_parsing 0 2>&1 | grep -o "test_02270_2.csv"

user_files_path=$($CLICKHOUSE_CLIENT --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep -E '^Code: 107.*FILE_DOESNT_EXIST' | head -1 | awk '{gsub("/nonexist.txt","",$9); print $9}')

${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION file('test_02270_1.csv') SELECT 'Hello', 'World'"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION file('test_02270_2.csv') SELECT 'Error'"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('test_02270*.csv', 'CSV', 'a String, b String')" 2>&1 | grep -o -m1 "test_02270_2.csv"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('test_02270*.csv', 'CSV', 'a String, b String')" --input_format_parallel_parsing 0 2>&1 | grep -o -m1 "test_02270_2.csv"

${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION file('test_02270_1.csv.gz') SELECT 'Hello', 'World'"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION file('test_02270_2.csv.gz') SELECT 'Error'"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('test_02270*.csv.gz', 'CSV', 'a String, b String')" 2>&1 | grep -o -m1 "test_02270_2.csv.gz"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('test_02270*.csv.gz', 'CSV', 'a String, b String')" --input_format_parallel_parsing 0 2>&1 | grep -o -m1 "test_02270_2.csv.gz"

rm "${CLICKHOUSE_TMP}"/test_02270_1.csv
rm "${CLICKHOUSE_TMP}"/test_02270_2.csv
rm "${user_files_path}"/test_02270_1.csv
rm "${user_files_path}"/test_02270_2.csv
rm "${user_files_path}"/test_02270_1.csv.gz
rm "${user_files_path}"/test_02270_2.csv.gz
