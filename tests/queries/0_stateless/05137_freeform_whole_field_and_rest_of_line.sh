#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

trap 'rm -f "$DATA_FILE"' EXIT

echo "A field read into a column of the provided structure has to be consumed entirely"

echo "1x,2" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 UInt64, c1 UInt64')" 2>&1 | grep -oF "INCORRECT_DATA" | head -1
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 String, c1 UInt64')"

printf '01\t2\n' > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 UInt64, c1 UInt64')"

echo "Trailing whitespace of a CSV or JSON field is allowed after the value"

echo '"1" , "2"' > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 UInt64, c1 UInt64')"

echo "The remainder of a line after a token ending with a colon is one verbatim field"

printf 'app: \\N\napp: a\\tb\napp: a\tb c\napp: 42\n' > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform') order by all"
$CLICKHOUSE_CLIENT -q "select c1, length(c1) from file('$FILE_NAME', 'Freeform', 'c0 String, c1 String') order by all"

echo "The remainder of a line is deserialized as a whole text into the column of the provided structure"

printf 'value: 1\nvalue: 2\n' > $DATA_FILE

$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 String, c1 UInt64') order by all"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 String, c1 Nullable(UInt64)') order by all"

printf 'value: 1\nvalue: NULL\n' > $DATA_FILE

$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 String, c1 Nullable(UInt64)') order by all"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 String, c1 UInt64') order by all"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform', 'c0 String, c1 UInt64') order by all settings input_format_null_as_default = 0" 2>&1 | grep -oF "UNEXPECTED_DATA_AFTER_PARSED_VALUE" | head -1
