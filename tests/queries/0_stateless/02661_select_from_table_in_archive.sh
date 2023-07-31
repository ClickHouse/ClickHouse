#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS 02661_archive_table"

user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

echo -e "1,2\n3,4" > ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv
zip ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.zip ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null
zip ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.zip  ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null

function read_archive_file() {
    $CLICKHOUSE_LOCAL --query "SELECT $1 FROM file('${user_files_path}/$2')"
    $CLICKHOUSE_CLIENT --query "CREATE TABLE 02661_archive_table Engine=File('CSV', '${user_files_path}/$2')"
    $CLICKHOUSE_CLIENT --query "SELECT $1 FROM 02661_archive_table"
    $CLICKHOUSE_CLIENT --query "DROP TABLE 02661_archive_table"
}

read_archive_file "*" "${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.zip :: ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv"
read_archive_file "c1" "${CLICKHOUSE_TEST_UNIQUE_NAME}_archive{1..2}.zip :: ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv"

rm ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv
rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.zip
rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.zip
