#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SAMPLE_FILE=$(mktemp 01947_multiple_pipe_read_sample_data_XXXXXX.csv)
STD_ERROR_CAPTURED=$(mktemp 01947_multiple_pipe_read_std_error_captured_XXXXXX.log)

echo 'File generated:'
${CLICKHOUSE_LOCAL} -q "SELECT number, if(number in (4,6), 'AAA', 'BBB') from numbers(7) FORMAT CSV" --format_csv_delimiter=, >"$SAMPLE_FILE"
cat "$SAMPLE_FILE"

echo '******************'
echo 'Attempt to read twice from a regular file'
${CLICKHOUSE_LOCAL} --structure 'key String' -q 'select * from table; select * from table;' --file "$SAMPLE_FILE"

echo '******************'
echo 'Attempt to read twice from a pipe'
echo 1 | ${CLICKHOUSE_LOCAL} --structure "a int" --query "select a from table where a in (select a from table)" 2>"$STD_ERROR_CAPTURED"
expected_error_message='Cannot read from a pipe twice'
cat "$STD_ERROR_CAPTURED" | grep -q "$expected_error_message" && echo "OK: stderr contains a message '$expected_error_message'" || echo "FAILED: Error message is wrong"

rm "$SAMPLE_FILE" "$STD_ERROR_CAPTURED"
