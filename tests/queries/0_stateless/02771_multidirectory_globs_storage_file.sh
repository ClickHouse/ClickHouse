#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel

#SELECT *, _file FROM file('02771/dir?/{subdir?1/data1,subdir2?/data2}.csv', CSV) WHERE _file == 'data1.csv';
#SELECT *, _file FROM file('02771/dir?/{subdir?1/data1,subdir2?/data2}.csv', CSV) WHERE _file == 'data2.csv';

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Data preparation.
# Now we can get the user_files_path by use the table file function for trick. also we can get it by query as:
#  "insert into function file('exist.txt', 'CSV', 'val1 char') values ('aaaa'); select _path from file('exist.txt', 'CSV', 'val1 char')"
user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/*
mkdir -p ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir{?/subdir?1/da,2/subdir2?/da}ta/non_existing.csv', CSV);" 2>&1 | grep -q "CANNOT_EXTRACT_TABLE_STRUCTURE" && echo 'OK' || echo 'FAIL'

# Create two files in different directories
mkdir -p ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir1/subdir11/
mkdir -p ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir2/subdir22/

echo 'This is file data1' > ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir1/subdir11/data1.csv
echo 'This is file data2' > ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir2/subdir22/data2.csv

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir{?/subdir?1/da,2/subdir2?/da}ta1.csv', CSV);"
${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir{?/subdir?1/da,2/subdir2?/da}ta2.csv', CSV);"

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir?/{subdir?1/data1,subdir2?/data2}.csv', CSV) WHERE _file == 'data1.csv';"
${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir?/{subdir?1/data1,subdir2?/data2}.csv', CSV) WHERE _file == 'data2.csv';"

# Add a directory to test against permission_denied
rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir_inaccessible/ && mkdir -p ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir_inaccessible/ && chmod 000 ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir_inaccessible/

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir{?/subdir?1/da,2/subdir2?/da}ta1.csv', CSV) SETTINGS ignore_eacces_multidirectory_globs = 0;"  2>&1 | grep -q "Permission denied" && echo 'OK' || echo 'FAIL'
${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir{?/subdir?1/da,2/subdir2?/da}ta1.csv', CSV) SETTINGS ignore_eacces_multidirectory_globs = 1;"

rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
