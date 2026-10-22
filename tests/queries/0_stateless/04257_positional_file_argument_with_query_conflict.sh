#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104735:
# Positional query files must not silently hide the --query argument.
# clickhouse-local executes the file then the query; clickhouse-client still rejects.

TEMP_SQL_FILE_NAME="${CLICKHOUSE_TMP}/04257_positional_file_argument_$$.sql"
trap 'rm -f "$TEMP_SQL_FILE_NAME"' EXIT

echo "SELECT 'from file';" > "$TEMP_SQL_FILE_NAME"

$CLICKHOUSE_LOCAL "$TEMP_SQL_FILE_NAME" -q "SELECT 'from query'" < /dev/null
$CLICKHOUSE_LOCAL "$TEMP_SQL_FILE_NAME" --query "SELECT 'from query'" < /dev/null

$CLICKHOUSE_CLIENT "$TEMP_SQL_FILE_NAME" -q "SELECT 'from query'" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT "$TEMP_SQL_FILE_NAME" --query "SELECT 'from query'" 2>&1 | grep -o 'BAD_ARGUMENTS'

# The --copy shortcut cannot be combined with a positional query file.
$CLICKHOUSE_LOCAL --copy "$TEMP_SQL_FILE_NAME" < /dev/null 2>&1 | grep -o 'BAD_ARGUMENTS'
