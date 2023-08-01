#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS 02661_archive_table"

user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

echo -e "1,2\n3,4" > ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv

function read_archive_file() {
    $CLICKHOUSE_LOCAL --query "SELECT $1 FROM file('${user_files_path}/$2')"
    $CLICKHOUSE_CLIENT --query "CREATE TABLE 02661_archive_table Engine=File('CSV', '${user_files_path}/$2')"
    $CLICKHOUSE_CLIENT --query "SELECT $1 FROM 02661_archive_table"
    $CLICKHOUSE_CLIENT --query "DROP TABLE 02661_archive_table"
}

function run_archive_test() {
    echo "Running for $1 files"
    read_archive_file "*" "${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.$1 :: ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv"
    read_archive_file "c1" "${CLICKHOUSE_TEST_UNIQUE_NAME}_archive{1..2}.$1 :: ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv"
}

zip ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.zip ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null
zip ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.zip  ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null

run_archive_test "zip"

rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.zip
rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.zip

tar -cvzf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.tar.gz ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null
tar -cvzf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.tar.gz ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null

run_archive_test "tar.gz"

rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.tar.gz
rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.tar.gz

tar -cvf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.tar ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null
tar -cvf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.tar ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null

run_archive_test "tar"

rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.tar
rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.tar

7z a ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.7z ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null 
7z a ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.7z ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null 

run_archive_test "7z"

rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.7z
rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.7z

rm ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv
