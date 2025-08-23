#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

# Test the segment fault issue when reading ORC files
# Fixes issue #85292

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

[ -e "${USER_FILES_PATH}"/test.data ] && rm "${USER_FILES_PATH}"/test.data

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t0"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t0 (c0 Int) ENGINE = File(ORC, '${USER_FILES_PATH}/test.data')"

${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE t0 (c0) VALUES (1),(2),(3),(5.4)"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${USER_FILES_PATH}/test.data', 'ORC') AS tx SETTINGS input_format_orc_use_fast_decoder = 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t0"

rm -f "${USER_FILES_PATH}"/test.data
