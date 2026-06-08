#!/usr/bin/env bash
# Tags: no-replicated-database

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

rm -rf ${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*
mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir{?/subdir?1/da,2/subdir2?/da}ta/non_existing.csv', CSV);" 2>&1 | grep -q "CANNOT_EXTRACT_TABLE_STRUCTURE" && echo 'OK' || echo 'FAIL'

# Create two files in different directories
mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir1/subdir11/
mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir2/subdir22/

echo 'This is file data1' > ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir1/subdir11/data1.csv
echo 'This is file data2' > ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir2/subdir22/data2.csv

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir{?/subdir?1/da,2/subdir2?/da}ta1.csv', CSV);"
${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir{?/subdir?1/da,2/subdir2?/da}ta2.csv', CSV);"

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir?/{subdir?1/data1,subdir2?/data2}.csv', CSV) WHERE _file == 'data1.csv';"
${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir?/{subdir?1/data1,subdir2?/data2}.csv', CSV) WHERE _file == 'data2.csv';"

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir3/subdir31/
echo 'This is file data3' > ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/dir3/subdir31/data3.csv
echo 'This is file data_root' > ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/data_root.csv

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/**/*.csv', CSV) ORDER BY _file;"
${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/**/data3.csv', CSV);"

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/to/a/
mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/to/b/sub/
echo 'This is file data4' > ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/to/a/file.csv
echo 'This is file data5' > ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/to/b/sub/file.csv
echo 'This is file data6' > ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/to/a/other.csv

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/to/**/file.csv', CSV) ORDER BY _file;"
${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/to/**/*.csv', CSV) ORDER BY _file;"
${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/**/file.csv', CSV) ORDER BY _file;"

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/zipdir/
echo 'This is file data_zip' > ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/zipdir/file.csv
(cd ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/zipdir/ && zip -q ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/a.zip file.csv)
rm -rf ${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/path/zipdir/

${CLICKHOUSE_CLIENT} --query "SELECT *, _file FROM file('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/path/**/a.zip::file.csv', CSV) ORDER BY _file;"

rm -rf ${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
