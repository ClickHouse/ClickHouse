#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

UNIX_ENDINGS="${CLICKHOUSE_TEST_UNIQUE_NAME}_data_without_crlf.tsv"
DOS_ENDINGS="${CLICKHOUSE_TEST_UNIQUE_NAME}_data_with_crlf.tsv"
DATA_FILE_UNIX_ENDINGS="${USER_FILES_PATH:?}/${UNIX_ENDINGS}"
DATA_FILE_DOS_ENDINGS="${USER_FILES_PATH:?}/${DOS_ENDINGS}"

touch $DATA_FILE_UNIX_ENDINGS
touch $DATA_FILE_DOS_ENDINGS    

echo -ne "Akiba_Hebrew_Academy\t2017-08-01\t241\nAegithina_tiphia\t2018-02-01\t34\n1971-72_Utah_Stars_season\t2016-10-01\t1\n" > $DATA_FILE_UNIX_ENDINGS
echo -ne "Akiba_Hebrew_Academy\t2017-08-01\t241\r\nAegithina_tiphia\t2018-02-01\t34\r\n1971-72_Utah_Stars_season\t2016-10-01\t1\r\n" > $DATA_FILE_DOS_ENDINGS

echo -e "<-- Read UNIX endings -->\n"
$CLICKHOUSE_CLIENT --query "SELECT * FROM file(${UNIX_ENDINGS}, 'TabSeparated', 'SearchTerm String, Date Date, Hits UInt32');" 
$CLICKHOUSE_CLIENT --query "SELECT * FROM file(${DOS_ENDINGS}, 'TabSeparated', 'SearchTerm String, Date Date, Hits UInt32'); --{ serverError INCORRECT_DATA }" 

echo -e "\n<-- Read DOS endings with setting input_format_tsv_crlf_end_of_line=1 -->\n"
$CLICKHOUSE_CLIENT --query "SELECT * FROM file(${DOS_ENDINGS}, 'TabSeparated', 'SearchTerm String, Date Date, Hits UInt32') SETTINGS input_format_tsv_crlf_end_of_line = 1;"

# Test teardown 
rm $DATA_FILE_UNIX_ENDINGS
rm $DATA_FILE_DOS_ENDINGS
