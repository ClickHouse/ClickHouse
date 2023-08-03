#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./02661_read_from_archive.lib
. "$CUR_DIR"/02661_read_from_archive.lib

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS 02661_archive_table"

user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

echo -e "1,2\n3,4" > ${CLICKHOUSE_TEST_UNIQUE_NAME}_data1.csv
echo -e "5,6\n7,8" > ${CLICKHOUSE_TEST_UNIQUE_NAME}_data2.csv
echo -e "9,10\n11,12" > ${CLICKHOUSE_TEST_UNIQUE_NAME}_data3.csv

function run_archive_test() {
    echo "Running for $1 files"

    eval "$2 ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive1.$1 ${CLICKHOUSE_TEST_UNIQUE_NAME}_data1.csv ${CLICKHOUSE_TEST_UNIQUE_NAME}_data2.csv > /dev/null"
    eval "$2 ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive2.$1 ${CLICKHOUSE_TEST_UNIQUE_NAME}_data1.csv ${CLICKHOUSE_TEST_UNIQUE_NAME}_data3.csv > /dev/null"
    eval "$2 ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive3.$1 ${CLICKHOUSE_TEST_UNIQUE_NAME}_data2.csv ${CLICKHOUSE_TEST_UNIQUE_NAME}_data3.csv > /dev/null"

    echo "archive{1,2} data{1,3}.csv"
    read_archive_file "${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive{1,2}.$1 :: ${CLICKHOUSE_TEST_UNIQUE_NAME}_data{1,3}.csv"
    echo "archive3 data*.csv"
    read_archive_file "${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive3.$1 :: ${CLICKHOUSE_TEST_UNIQUE_NAME}_data*.csv"
    echo "archive* *.csv"
    read_archive_file "${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive*.$1 :: *.csv"
    echo "archive* {2..3}.csv"
    read_archive_file "${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive*.$1 :: ${CLICKHOUSE_TEST_UNIQUE_NAME}_data{2..3}.csv"

    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive3.$1::{2..3}.csv')" 2>&1 | grep -q "CANNOT_UNPACK_ARCHIVE" && echo "OK" || echo "FAIL"

    rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive1.$1
    rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive2.$1
    rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_with_globs_archive3.$1
}

run_archive_test "zip" "zip"
run_archive_test "tar.gz" "tar -cvzf"
run_archive_test "tar" "tar -cvf"
run_archive_test "7z" "7z a"

rm ${CLICKHOUSE_TEST_UNIQUE_NAME}_data{1..3}.csv