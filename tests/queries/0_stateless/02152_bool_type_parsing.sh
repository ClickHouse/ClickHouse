#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02152.data
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

echo -e "Custom true\nCustom false\nYes\nNo\nyes\nno\ny\nY\nN\nTrue\nFalse\ntrue\nfalse\nt\nf\nT\nF\nOn\nOff\non\noff\nenable\ndisable\nenabled\ndisabled" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FILE_NAME', 'TSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FILE_NAME', 'TSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false', input_format_parallel_parsing=0, max_read_buffer_size=2"

$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FILE_NAME', 'CSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FILE_NAME', 'CSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false', input_format_parallel_parsing=0, max_read_buffer_size=2"

echo -e "'Yes'\n'No'\n'yes'\n'no'\n'y'\n'Y'\n'N'\nTrue\nFalse\ntrue\nfalse\n't'\n'f'\n'T'\n'F'\n'On'\n'Off'\n'on'\n'off'\n'enable'\n'disable'\n'enabled'\n'disabled'" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FILE_NAME', 'CustomSeparated', 'bool Bool') settings format_custom_escaping_rule='Quoted'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$FILE_NAME', 'CustomSeparated', 'bool Bool') settings format_custom_escaping_rule='Quoted', input_format_parallel_parsing=0, max_read_buffer_size=2"

rm $DATA_FILE

