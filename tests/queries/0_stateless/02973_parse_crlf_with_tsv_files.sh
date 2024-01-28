#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

# Test setup
touch ${USER_FILES_PATH:?}/02973_data_without_crlf.tsv
touch ${USER_FILES_PATH:?}/02973_data_with_crlf.tsv
echo -e 'Akiba_Hebrew_Academy\t2017-08-01\t241\nAegithina_tiphia\t2018-02-01\t34\n1971-72_Utah_Stars_season\t2016-10-01\t1' > "$USER_FILES_PATH/02973_data_without_crlf.tsv"
echo -e 'Akiba_Hebrew_Academy\t2017-08-01\t241\r\nAegithina_tiphia\t2018-02-01\t34\r\n1971-72_Utah_Stars_season\t2016-10-01\t1\r' > "$USER_FILES_PATH/02973_data_with_crlf.tsv"

$CLICKHOUSE_CLIENT --multiquery "SELECT * FROM file(02973_data_without_crlf.tsv, 'TabSeparated', 'SearchTerm String, Date Date, Hits UInt32');" 
$CLICKHOUSE_CLIENT --multiquery "SELECT * FROM file(02973_data_with_crlf.tsv, 'TabSeparated', 'SearchTerm String, Date Date, Hits UInt32'); --{clientError 117}"

# Change setting to escape \r 
$CLICKHOUSE_CLIENT --multiquery "SELECT * FROM file(02973_data_with_crlf.tsv, 'TabSeparated', 'SearchTerm String, Date Date, Hits UInt32');"

# Test teardown 
rm "$USER_FILES_PATH/02973_data_without_crlf.tsv" 
rm "$USER_FILES_PATH/02973_data_with_crlf.tsv"
