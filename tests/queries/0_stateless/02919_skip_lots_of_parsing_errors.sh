#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE=$CLICKHOUSE_TEST_UNIQUE_NAME
ERRORS_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.errors

$CLICKHOUSE_LOCAL -q "select 'Error' from numbers(100000) format TSVRaw" > $FILE
echo -e "42" >> $FILE

$CLICKHOUSE_LOCAL -q "select * from file('$FILE', CSV, 'x UInt32') settings input_format_allow_errors_ratio=1, max_block_size=10000, input_format_parallel_parsing=0, input_format_record_errors_file_path='$ERRORS_FILE'";
$CLICKHOUSE_LOCAL -q "select count() from file('$ERRORS_FILE', CSV)"
rm $ERRORS_FILE

$CLICKHOUSE_LOCAL -q "select * from file('$FILE', CSV, 'x UInt32') settings input_format_allow_errors_ratio=1, max_block_size=10000, input_format_parallel_parsing=1, input_format_record_errors_file_path='$ERRORS_FILE'";
$CLICKHOUSE_LOCAL -q "select count() from file('$ERRORS_FILE', CSV)"
rm $ERRORS_FILE

rm $FILE
