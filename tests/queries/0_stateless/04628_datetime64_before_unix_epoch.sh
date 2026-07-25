#!/usr/bin/env bash
# Tags: no-fasttest
# Pre-UNIX epoch DateTime64 values should serialize to protobuf int64 without overflow.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "${CLICKHOUSE_TMP}" || exit 1

FILE_BEFORE="${CLICKHOUSE_TEST_UNIQUE_NAME}_before.pb"
FILE_AFTER="${CLICKHOUSE_TEST_UNIQUE_NAME}_after.pb"
trap 'rm -f "${FILE_BEFORE}" "${FILE_AFTER}"' EXIT

${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_BEFORE}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
1000-01-01 00:00:00+00 "

${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_BEFORE}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"

${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FILE_AFTER}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')
SETTINGS date_time_input_format = 'best_effort', engine_file_truncate_on_insert = 1
FORMAT TSV
3000-01-01 00:00:00+00 "

${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE_AFTER}', 'Protobuf', 't DateTime64(3, \\'UTC\\')')"
