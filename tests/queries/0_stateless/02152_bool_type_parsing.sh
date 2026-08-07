#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_02103_null.data
trap 'rm -rf ${DATA_FILE}' EXIT

# Wrapper for clickhouse-client to always output in JSONEachRow format, that
# way format settings will not affect output.
function clickhouse_local()
{
    $CLICKHOUSE_LOCAL --output-format JSONEachRow "$@"
}

echo -e "Custom true\nCustom false\nYes\nNo\nyes\nno\ny\nY\nN\nTrue\nFalse\ntrue\nfalse\nt\nf\nT\nF\nOn\nOff\non\noff\nenable\ndisable\nenabled\ndisabled" > $DATA_FILE

clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false'"
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false', input_format_parallel_parsing=0, max_read_buffer_size=2"

clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false'"
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 'bool Bool') settings bool_true_representation='Custom true', bool_false_representation='Custom false', input_format_parallel_parsing=0, max_read_buffer_size=2"

echo -e "'Yes'\n'No'\n'yes'\n'no'\n'y'\n'Y'\n'N'\nTrue\nFalse\ntrue\nfalse\n't'\n'f'\n'T'\n'F'\n'On'\n'Off'\n'on'\n'off'\n'enable'\n'disable'\n'enabled'\n'disabled'" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CustomSeparated', 'bool Bool') settings format_custom_escaping_rule='Quoted'"
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CustomSeparated', 'bool Bool') settings format_custom_escaping_rule='Quoted', input_format_parallel_parsing=0, max_read_buffer_size=2"
