#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE_NAME=$CLICKHOUSE_TEST_UNIQUE_NAME.csv.xz
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

cp "$CURDIR"/data_csv/1m_rows_cr_end_of_line.csv.xz $DATA_FILE

$CLICKHOUSE_CLIENT -q "SELECT count(1) from file('$FILE_NAME') settings input_format_csv_allow_cr_end_of_line=1, optimize_count_from_files=1"
$CLICKHOUSE_CLIENT -q "SELECT count(1) from file('$FILE_NAME') settings input_format_csv_allow_cr_end_of_line=1, optimize_count_from_files=0"

rm $DATA_FILE
