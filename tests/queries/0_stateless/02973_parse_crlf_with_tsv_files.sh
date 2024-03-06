#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Data preparation step
USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME_UNIX="${CLICKHOUSE_TEST_UNIQUE_NAME}_data_without_crlf.tsv"
FILE_NAME_CRLF="${CLICKHOUSE_TEST_UNIQUE_NAME}_data_with_crlf.tsv"
DATA_FILE_UNIX_ENDINGS=${USER_FILES_PATH:?}/FILE_NAME_UNIX
DATA_FILE_DOS_ENDINGS=${USER_FILES_PATH:?}/FILE_NAME_CRLF

touch $DATA_FILE_UNIX_ENDINGS
touch $DATA_FILE_DOS_ENDINGS    

echo -ne "Akiba_Hebrew_Academy\t2017-08-01\t241\nAegithina_tiphia\t2018-02-01\t34\n1971-72_Utah_Stars_season\t2016-10-01\t1\n" > $DATA_FILE_UNIX_ENDINGS
echo -ne "Akiba_Hebrew_Academy\t2017-08-01\t241\r\nAegithina_tiphia\t2018-02-01\t34\r\n1971-72_Utah_Stars_season\t2016-10-01\t1\r\n" > $DATA_FILE_DOS_ENDINGS

echo -e "<-- Read UNIX endings -->\n"
$CLICKHOUSE_CLIENT --query "SELECT * FROM file(${FILE_NAME_UNIX}, 'TabSeparated', 'SearchTerm String, Date Date, Hits UInt32');" 
$CLICKHOUSE_CLIENT --multiquery --query "SELECT * FROM file(${FILE_NAME_CRLF}, 'TabSeparated', 'SearchTerm String, Date Date, Hits UInt32'); --{serverError 117}" 

echo -e "\n<-- Read DOS endings with setting input_format_tsv_crlf_end_of_line=1 -->\n"
$CLICKHOUSE_CLIENT --query "SELECT * FROM file(${FILE_NAME_CRLF}, 'TabSeparated', 'SearchTerm String, Date Date, Hits UInt32') SETTINGS input_format_tsv_crlf_end_of_line = 1;"

# Test teardown 
rm $DATA_FILE_UNIX_ENDINGS
rm $DATA_FILE_DOS_ENDINGS
