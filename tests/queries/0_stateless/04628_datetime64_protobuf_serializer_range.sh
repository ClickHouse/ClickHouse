#!/usr/bin/env bash
# Tags: no-fasttest
# DateTime64 Protobuf auto-schema stores scaled ticks (int64), so pre-epoch values,
# subsecond precision, and upper bound values are preserved through serialization and deserialization.
# Legacy whole-seconds files can still be read with input_format_protobuf_datetime64_legacy_seconds=1.

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
trap 'rm -f "${FILE_BEFORE}" "${FILE_AFTER}" "${FILE_MAX}" "${FILE_PAST_MAX}" "${FILE_FRAC_EPOCH}" "${FILE_FRAC}" "${FILE_LEGACY_SECONDS}"' EXIT

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
