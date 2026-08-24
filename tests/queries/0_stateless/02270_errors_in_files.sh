#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

prefix=${CLICKHOUSE_TEST_UNIQUE_NAME}

[ -e "${CLICKHOUSE_TMP}"/${prefix}_1.csv ] && rm "${CLICKHOUSE_TMP}"/${prefix}_1.csv
[ -e "${CLICKHOUSE_TMP}"/${prefix}_2.csv ] && rm "${CLICKHOUSE_TMP}"/${prefix}_2.csv

echo "Hello,World" > "${CLICKHOUSE_TMP}"/${prefix}_1.csv
echo "Error" > "${CLICKHOUSE_TMP}"/${prefix}_2.csv

${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${CLICKHOUSE_TMP}/${prefix}*.csv', CSV, 'a String, b String')" 2>&1 | grep -o "${prefix}_2.csv" | sed "s/${prefix}/test_02270/"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${CLICKHOUSE_TMP}/${prefix}*.csv', CSV, 'a String, b String')" --input_format_parallel_parsing 0 2>&1 | grep -o "${prefix}_2.csv" | sed "s/${prefix}/test_02270/"

${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION file('${prefix}_1.csv') SELECT 'Hello', 'World'"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION file('${prefix}_2.csv') SELECT 'Error'"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${prefix}*.csv', 'CSV', 'a String, b String')" 2>&1 | grep -o -m1 "${prefix}_2.csv" | sed "s/${prefix}/test_02270/"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${prefix}*.csv', 'CSV', 'a String, b String')" --input_format_parallel_parsing 0 2>&1 | grep -o -m1 "${prefix}_2.csv" | sed "s/${prefix}/test_02270/"

${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION file('${prefix}_1.csv.gz') SELECT 'Hello', 'World'"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION file('${prefix}_2.csv.gz') SELECT 'Error'"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${prefix}*.csv.gz', 'CSV', 'a String, b String')" 2>&1 | grep -o -m1 "${prefix}_2.csv.gz" | sed "s/${prefix}/test_02270/"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${prefix}*.csv.gz', 'CSV', 'a String, b String')" --input_format_parallel_parsing 0 2>&1 | grep -o -m1 "${prefix}_2.csv.gz" | sed "s/${prefix}/test_02270/"

rm -f "${CLICKHOUSE_TMP}"/${prefix}_1.csv
rm -f "${CLICKHOUSE_TMP}"/${prefix}_2.csv
rm -f "${USER_FILES_PATH}"/${prefix}_1.csv
rm -f "${USER_FILES_PATH}"/${prefix}_2.csv
rm -f "${USER_FILES_PATH}"/${prefix}_1.csv.gz
rm -f "${USER_FILES_PATH}"/${prefix}_2.csv.gz
