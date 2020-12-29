#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

SAMPLE_FILE="$CURDIR/01355_sample_data.csv"
STD_ERROR_CAPTURED="$CURDIR/01355_std_error_captured.log"

echo 'File generated:'
${CLICKHOUSE_LOCAL} -q "SELECT number, if(number in (4,6), 'AAAAAAA', '0') from numbers(7) FORMAT TSV" | tr '\t' ',' >"$SAMPLE_FILE"
cat "$SAMPLE_FILE"

echo '******************'
echo 'attempt to parse w/o flags'
cat "$SAMPLE_FILE" | clickhouse-local --input-format=CSV --structure='num1 Int64, num2 Int64' --query='SELECT * from table' 2>"$STD_ERROR_CAPTURED"
echo "Return code: $?"
expected_error_message='is not like Int64'
cat "$STD_ERROR_CAPTURED" | grep -q "$expected_error_message" && echo "OK: stderr contains a message '$expected_error_message'" || echo "FAILED: Error message is wrong"

echo '******************'
echo 'attempt to parse with input_format_allow_errors_ratio=0.1'
cat "$SAMPLE_FILE" | clickhouse-local --input-format=CSV --structure='num1 Int64, num2 Int64' --query='SELECT * from table' --input_format_allow_errors_ratio=0.1 2>"$STD_ERROR_CAPTURED"
echo "Return code: $?"
expected_error_message='Already have 1 errors out of 5 rows, which is 0.2'
cat "$STD_ERROR_CAPTURED" | grep -q "$expected_error_message" && echo "OK: stderr contains a message '$expected_error_message'" || echo "FAILED: Error message is wrong"

echo '******************'
echo 'attempt to parse with input_format_allow_errors_ratio=0.3'
cat "$SAMPLE_FILE" | clickhouse-local --input-format=CSV --structure='num1 Int64, num2 Int64' --query='SELECT * from table' --input_format_allow_errors_ratio=0.3 2>"$STD_ERROR_CAPTURED"
echo "Return code: $?"
cat "$STD_ERROR_CAPTURED"

echo '******************'
echo 'attempt to parse with input_format_allow_errors_num=1'
cat "$SAMPLE_FILE" | clickhouse-local --input-format=CSV --structure='num1 Int64, num2 Int64' --query='SELECT * from table' --input_format_allow_errors_num=1 2>"$STD_ERROR_CAPTURED"
echo "Return code: $?"
expected_error_message='Already have 2 errors out of 7 rows'
cat "$STD_ERROR_CAPTURED" | grep -q "$expected_error_message" && echo "OK: stderr contains a message '$expected_error_message'" || echo "FAILED: Error message is wrong"

echo '******************'
echo 'attempt to parse with input_format_allow_errors_num=2'
cat "$SAMPLE_FILE" | clickhouse-local --input-format=CSV --structure='num1 Int64, num2 Int64' --query='SELECT * from table' --input_format_allow_errors_num=2 2>"$STD_ERROR_CAPTURED"
echo "Return code: $?"
cat "$STD_ERROR_CAPTURED"

rm "$STD_ERROR_CAPTURED" "$SAMPLE_FILE"
