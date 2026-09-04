#!/usr/bin/env bash
# Tags: no-fasttest
# DateTime64 Protobuf auto-schema stores scaled ticks (int64), so pre-epoch values,
# subsecond precision, and upper bound values are preserved through serialization and deserialization.
# Legacy whole-seconds files can still be read with input_format_protobuf_datetime64_legacy_seconds=1
# (or SET compatibility = '26.8'). Legacy writers can emit whole Unix seconds with
# output_format_protobuf_datetime64_legacy_seconds=1.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "${CLICKHOUSE_TMP}" || exit 1

FILE_BEFORE="${CLICKHOUSE_TEST_UNIQUE_NAME}_before.pb"
FILE_AFTER="${CLICKHOUSE_TEST_UNIQUE_NAME}_after.pb"
FILE_MAX="${CLICKHOUSE_TEST_UNIQUE_NAME}_max.pb"
FILE_PAST_MAX="${CLICKHOUSE_TEST_UNIQUE_NAME}_past_max.pb"
FILE_FRAC_EPOCH="${CLICKHOUSE_TEST_UNIQUE_NAME}_frac_epoch.pb"
FILE_FRAC="${CLICKHOUSE_TEST_UNIQUE_NAME}_frac.pb"
FILE_LEGACY_SECONDS="${CLICKHOUSE_TEST_UNIQUE_NAME}_legacy_seconds.pb"
FILE_LEGACY_OUT="${CLICKHOUSE_TEST_UNIQUE_NAME}_legacy_out.pb"
FILE_LEGACY_DOUBLE="${CLICKHOUSE_TEST_UNIQUE_NAME}_legacy_double.pb"
trap 'rm -f "${FILE_BEFORE}" "${FILE_AFTER}" "${FILE_MAX}" "${FILE_PAST_MAX}" "${FILE_FRAC_EPOCH}" "${FILE_FRAC}" "${FILE_LEGACY_SECONDS}" "${FILE_LEGACY_OUT}" "${FILE_LEGACY_DOUBLE}"' EXIT

echo '-- pre-epoch'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_BEFORE}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
1000-01-01 00:00:00+00"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_BEFORE}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"

echo '-- post-epoch'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_AFTER}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
3000-01-01 00:00:00+00"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_AFTER}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"

echo '-- documented upper bound at precision 7'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_MAX}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
9999-12-31 23:59:59.9999999+00"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_MAX}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')"

echo '-- past upper bound at precision 7 (errs)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_PAST_MAX}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
10000-01-01 00:00:00+00; -- { serverError 41 }"

echo '-- subsecond precision near the Unix epoch'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_FRAC_EPOCH}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
1970-01-01 00:00:20.5555555+00"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_FRAC_EPOCH}', 'Protobuf', 't DateTime64(7, \\'UTC\\')')"

echo '-- subsecond precision for a regular timestamp'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_FRAC}', 'Protobuf', 't DateTime64(6, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
2024-06-15 12:34:56.123456+00"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_FRAC}', 'Protobuf', 't DateTime64(6, \\'UTC\\')')"

echo '-- legacy whole-seconds file (written as DateTime64(0) ticks == Unix seconds)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_LEGACY_SECONDS}', 'Protobuf', 't DateTime64(0, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
2020-01-01 00:00:00+00"

echo '-- legacy read without setting interprets seconds as ticks (previously incorrect behavior)'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_LEGACY_SECONDS}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"

echo '-- legacy read with setting restores the Unix-second instant'
${CLICKHOUSE_LOCAL} --query "
SELECT *
FROM file('${FILE_LEGACY_SECONDS}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_legacy_seconds = 1"

echo '-- compatibility 26.8 restores legacy whole-seconds decoding (produces 2020-01-01 00:00:00.000)'
${CLICKHOUSE_LOCAL} --query "
SELECT *
FROM file('${FILE_LEGACY_SECONDS}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS compatibility = '26.8'"

echo '-- legacy output writes whole Unix seconds (subseconds truncated)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_LEGACY_OUT}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1, output_format_protobuf_datetime64_legacy_seconds = 1
FORMAT TSV
2020-01-01 00:00:00.123+00"

echo '-- legacy output read as ticks without input setting (seconds misinterpreted as ticks)'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_LEGACY_OUT}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"

echo '-- legacy output and legacy input restores the truncated Unix-second instant'
${CLICKHOUSE_LOCAL} --query "
SELECT *
FROM file('${FILE_LEGACY_OUT}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS input_format_protobuf_datetime64_legacy_seconds = 1"

DOUBLE_SCHEMA='syntax = "proto3"; message Row { double t = 1; }'

echo '-- legacy double field stores fractional Unix seconds'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_LEGACY_DOUBLE}', 'Protobuf')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row',
         engine_file_truncate_on_insert = 1
SELECT CAST(1577836800.125 AS Float64) AS t"

echo '-- legacy double read preserves subseconds'
${CLICKHOUSE_LOCAL} --query "
SELECT *
FROM file('${FILE_LEGACY_DOUBLE}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row',
         input_format_protobuf_datetime64_legacy_seconds = 1"

echo '-- legacy double write intermediately preserves subseconds'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_LEGACY_DOUBLE}', 'Protobuf')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row',
         engine_file_truncate_on_insert = 1,
         output_format_protobuf_datetime64_legacy_seconds = 1
SELECT toDateTime64('2020-01-01 00:00:00.125', 3, 'UTC') AS t"
${CLICKHOUSE_LOCAL} --query "
SELECT t
FROM file('${FILE_LEGACY_DOUBLE}', 'Protobuf', 't Float64')
SETTINGS format_schema_source = 'string',
         format_schema = '${DOUBLE_SCHEMA}',
         format_schema_message_name = 'Row'"