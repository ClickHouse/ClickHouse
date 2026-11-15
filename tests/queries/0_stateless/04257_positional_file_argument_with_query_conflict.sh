#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104735
# A positional file argument is converted to --queries-file internally and
# combining it with --query (-q) must be rejected with BAD_ARGUMENTS, not
# silently ignore the -q query.

TEMP_SQL_FILE_NAME="${CLICKHOUSE_TMP}/04257_positional_file_argument_$$.sql"
trap 'rm -f "$TEMP_SQL_FILE_NAME"' EXIT

echo "SELECT 'from file';" > "$TEMP_SQL_FILE_NAME"

$CLICKHOUSE_LOCAL "$TEMP_SQL_FILE_NAME" -q "SELECT 'from query'" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_LOCAL "$TEMP_SQL_FILE_NAME" --query "SELECT 'from query'" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT "$TEMP_SQL_FILE_NAME" -q "SELECT 'from query'" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT "$TEMP_SQL_FILE_NAME" --query "SELECT 'from query'" 2>&1 | grep -o 'BAD_ARGUMENTS'
