#!/usr/bin/env bash
# Tags: no-fasttest

# Test the segment fault issue when reading ORC files
# Fixes issue #85292

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TEST_FILE="orc_file_${RANDOM}${RANDOM}.data"
TABLE_NAME="orc_table_${RANDOM}${RANDOM}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_NAME} SYNC"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_NAME} (c0 Int) ENGINE = File(ORC, '${USER_FILES_PATH}/${TEST_FILE}')"

${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE ${TABLE_NAME} (c0) VALUES (1),(2),(3),(5.4)"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${USER_FILES_PATH}/${TEST_FILE}', 'ORC') AS tx SETTINGS input_format_orc_use_fast_decoder = 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_NAME} sync"

rm -f "${USER_FILES_PATH}/${TEST_FILE}"
