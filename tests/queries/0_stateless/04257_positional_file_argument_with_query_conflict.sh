#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104735:
# Positional query files must not silently hide the --query argument.
# clickhouse-local executes the file then the query; clickhouse-client still rejects.

TEMP_SQL_FILE_NAME="${CLICKHOUSE_TMP}/04257_positional_file_argument_$$.sql"
STDIN_DATA_FILE_NAME="${CLICKHOUSE_TMP}/04257_positional_file_stdin_$$.tsv"
trap 'rm -f "$TEMP_SQL_FILE_NAME" "$STDIN_DATA_FILE_NAME"' EXIT

echo "SELECT 'from file';" > "$TEMP_SQL_FILE_NAME"

$CLICKHOUSE_LOCAL "$TEMP_SQL_FILE_NAME" -q "SELECT 'from query'" < /dev/null
$CLICKHOUSE_LOCAL "$TEMP_SQL_FILE_NAME" --query "SELECT 'from query'" < /dev/null
# A command-line query before an explicitly named query file still runs last.
$CLICKHOUSE_LOCAL -q "SELECT 'from query'" --queries-file="$TEMP_SQL_FILE_NAME" < /dev/null

$CLICKHOUSE_CLIENT "$TEMP_SQL_FILE_NAME" -q "SELECT 'from query'" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT "$TEMP_SQL_FILE_NAME" --query "SELECT 'from query'" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Reject --copy with both an explicit and a positional query-file argument.
$CLICKHOUSE_LOCAL --copy --queries-file="$TEMP_SQL_FILE_NAME" < /dev/null 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_LOCAL --copy "$TEMP_SQL_FILE_NAME" < /dev/null 2>&1 | grep -o 'BAD_ARGUMENTS'

# Regression: preparing an inferred table must not consume regular-file stdin.
printf '1\n2\n3\n' > "$STDIN_DATA_FILE_NAME"
printf '%s\n' 'CREATE TABLE stdin_source_04257 (x UInt8) ENGINE=File(TSV, stdin);' 'SELECT sum(x) FROM stdin_source_04257;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --storage_file_read_method=read --queries-file="$TEMP_SQL_FILE_NAME" < "$STDIN_DATA_FILE_NAME"
$CLICKHOUSE_LOCAL --storage_file_read_method=read --queries-file="$TEMP_SQL_FILE_NAME" -q 'SELECT 42 FROM system.one' < "$STDIN_DATA_FILE_NAME"

# The -q statement must be able to use a SQL UDF from its preceding query file.
echo 'CREATE FUNCTION fn_04257 AS (x) -> x + 1;' > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL -q 'SELECT fn_04257(41)' --queries-file="$TEMP_SQL_FILE_NAME" < /dev/null
