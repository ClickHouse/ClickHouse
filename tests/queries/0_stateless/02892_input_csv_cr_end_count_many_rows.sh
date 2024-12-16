#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep -E '^Code: 107.*FILE_DOESNT_EXIST' | head -1 | awk '{gsub("/nonexist.txt","",$9); print $9}')

cp "$CURDIR"/data_csv/1m_rows_cr_end_of_line.csv.xz $USER_FILES_PATH/

$CLICKHOUSE_CLIENT -q "SELECT count(1) from file('1m_rows_cr_end_of_line.csv.xz') settings input_format_csv_allow_cr_end_of_line=1, optimize_count_from_files=1"
$CLICKHOUSE_CLIENT -q "SELECT count(1) from file('1m_rows_cr_end_of_line.csv.xz') settings input_format_csv_allow_cr_end_of_line=1, optimize_count_from_files=0"

rm $USER_FILES_PATH/1m_rows_cr_end_of_line.csv.xz