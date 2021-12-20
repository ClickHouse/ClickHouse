#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02152.data
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

echo -e "Custom true\nCustom false\nYes\nNo\nyes\nno\ny\nY\nN\nTrue\nFalse\ntrue\nfalse\nt\nf\nT\nF\nOn\nOff\non\noff" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FILE_NAME', 'TSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FILE_NAME', 'CSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false'"

rm $DATA_FILE

