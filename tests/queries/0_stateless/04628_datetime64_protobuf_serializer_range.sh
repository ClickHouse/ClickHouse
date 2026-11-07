#!/usr/bin/env bash
# Tags: no-fasttest
# Integer Protobuf fields for DateTime64 are Unix seconds by default, values 
# decode to the same instant after the column precision changes; subseconds are truncated.
# Scaled ticks are opt-in: set input_format_protobuf_datetime64_scale and
# output_format_protobuf_datetime64_scale to the column precision. Mismatches is rejected.
# Float/double Protobuf fields always store fractional Unix seconds and ignore those settings.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "${CLICKHOUSE_TMP}" || exit 1

FILE_BEFORE="${CLICKHOUSE_TEST_UNIQUE_NAME}_before.pb"
FILE_AFTER="${CLICKHOUSE_TEST_UNIQUE_NAME}_after.pb"
FILE_MAX="${CLICKHOUSE_TEST_UNIQUE_NAME}_max.pb"
FILE_PAST_MAX="${CLICKHOUSE_TEST_UNIQUE_NAME}_past_max.pb"
FILE_SECONDS="${CLICKHOUSE_TEST_UNIQUE_NAME}_seconds.pb"
FILE_TRUNC="${CLICKHOUSE_TEST_UNIQUE_NAME}_trunc.pb"
FILE_FRAC_EPOCH="${CLICKHOUSE_TEST_UNIQUE_NAME}_frac_epoch.pb"
FILE_FRAC="${CLICKHOUSE_TEST_UNIQUE_NAME}_frac.pb"
FILE_TICKS="${CLICKHOUSE_TEST_UNIQUE_NAME}_ticks.pb"
FILE_SCALE0="${CLICKHOUSE_TEST_UNIQUE_NAME}_scale0.pb"
FILE_DOUBLE="${CLICKHOUSE_TEST_UNIQUE_NAME}_double.pb"
trap 'rm -f "${FILE_BEFORE}" "${FILE_AFTER}" "${FILE_MAX}" "${FILE_PAST_MAX}" "${FILE_SECONDS}" "${FILE_TRUNC}" "${FILE_FRAC_EPOCH}" "${FILE_FRAC}" "${FILE_TICKS}" "${FILE_SCALE0}" "${FILE_DOUBLE}"' EXIT

echo '-- pre-epoch (Unix seconds)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_BEFORE}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
1000-01-01 00:00:00+00"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_BEFORE}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"

echo '-- post-epoch (Unix seconds)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_AFTER}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
3000-01-01 00:00:00+00"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_AFTER}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"

echo '-- seconds written at precision 0 still decode at precision 3'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_SECONDS}', 'Protobuf', 't DateTime64(0, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
2020-01-01 00:00:00+00"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_SECONDS}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"

echo '-- default output truncates subseconds to Unix seconds'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_TRUNC}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
2020-01-01 00:00:00.123+00"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_TRUNC}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"

echo '-- documented upper bound at precision 7 (scaled ticks)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_MAX}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1, output_format_protobuf_datetime64_scale = 7
FORMAT TSV
9999-12-31 23:59:59.9999999+00"
${CLICKHOUSE_LOCAL} --query "
SELECT * FROM file('${FILE_MAX}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_scale = 7"

echo '-- past upper bound at precision 7 (errs)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_PAST_MAX}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1, output_format_protobuf_datetime64_scale = 7
FORMAT TSV
10000-01-01 00:00:00+00; -- { serverError 41 }"

echo '-- scaled ticks near the Unix epoch require a matching precision'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_FRAC_EPOCH}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1, output_format_protobuf_datetime64_scale = 7
FORMAT TSV
1970-01-01 00:00:20.5555555+00"
${CLICKHOUSE_LOCAL} --query "
SELECT * FROM file('${FILE_FRAC_EPOCH}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_scale = 7"

echo '-- scaled ticks for a regular timestamp require a matching precision'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_FRAC}', 'Protobuf', 't DateTime64(6, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1, output_format_protobuf_datetime64_scale = 6
FORMAT TSV
2024-06-15 12:34:56.123456+00"
${CLICKHOUSE_LOCAL} --query "
SELECT * FROM file('${FILE_FRAC}', 'Protobuf', 't DateTime64(6, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_scale = 6"

echo '-- output scale must match the column precision (errs)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_TICKS}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1, output_format_protobuf_datetime64_scale = 6
FORMAT TSV
2020-01-01 00:00:00.123+00; -- { serverError 36 }"

echo '-- input scale must match the column precision (errs)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_TICKS}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1, output_format_protobuf_datetime64_scale = 3
FORMAT TSV
2020-01-01 00:00:00.123+00"
${CLICKHOUSE_LOCAL} --query "
SELECT * FROM file('${FILE_TICKS}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_scale = 6; -- { serverError 36 }"

echo '-- reading ticks into a different column precision is rejected (errs)'
${CLICKHOUSE_LOCAL} --query "
SELECT * FROM file('${FILE_TICKS}', 'Protobuf', 't DateTime64(6, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_scale = 3; -- { serverError 36 }"

echo '-- matching ticks preserve subseconds'
${CLICKHOUSE_LOCAL} --query "
SELECT * FROM file('${FILE_TICKS}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_scale = 3"

echo '-- scale 0 ticks are Unix seconds'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_SCALE0}', 'Protobuf', 't DateTime64(0, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1, output_format_protobuf_datetime64_scale = 0
FORMAT TSV
2020-01-01 00:00:00+00"
${CLICKHOUSE_LOCAL} --query "
SELECT * FROM file('${FILE_SCALE0}', 'Protobuf', 't DateTime64(0, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_scale = 0"

echo '-- scale outside the DateTime64 range is rejected (errs)'
${CLICKHOUSE_LOCAL} --query "
SELECT * FROM file('${FILE_SECONDS}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_scale = 99; -- { serverError 36 }"

DOUBLE_SCHEMA='syntax = "proto3"; message Row { double t = 1; }'

echo '-- double field stores fractional Unix seconds (independent of the scale setting)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_DOUBLE}', 'Protobuf')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row',
         engine_file_truncate_on_insert = 1
SELECT CAST(1577836800.125 AS Float64) AS t"

echo '-- double read preserves subseconds without the scale setting'
${CLICKHOUSE_LOCAL} --query "
SELECT *
FROM file('${FILE_DOUBLE}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row'"

echo '-- double write preserves subseconds without the scale setting'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_DOUBLE}', 'Protobuf')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row',
         engine_file_truncate_on_insert = 1
SELECT toDateTime64('2020-01-01 00:00:00.125', 3, 'UTC') AS t"
${CLICKHOUSE_LOCAL} --query "
SELECT t
FROM file('${FILE_DOUBLE}', 'Protobuf', 't Float64')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row'"

echo '-- fractional double 1.5 is Unix seconds, not truncated ticks'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_DOUBLE}', 'Protobuf')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row',
         engine_file_truncate_on_insert = 1
SELECT CAST(1.5 AS Float64) AS t"
${CLICKHOUSE_LOCAL} --query "
SELECT *
FROM file('${FILE_DOUBLE}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row'"
