#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

## Use process ID ($$) for uniqueness of file name
TEMP_SQL_FILE_NAME=$"01523_client_local_queries_file_parameter_tmp_$$.sql"
echo "SELECT 1;" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" 2>&1

echo "CREATE TABLE 01523_test(value Int32) ENGINE=Log;
INSERT INTO 01523_test 
    VALUES (1), (2), (3);
SELECT * FROM 01523_test;
DROP TABLE 01523_test;" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" 2>&1

echo "CREATE TABLE 01523_test (a Int64, b Int64) ENGINE = File(CSV, stdin);
SELECT a, b FROM 01523_test;
DROP TABLE 01523_test;" > "$TEMP_SQL_FILE_NAME"

echo -e "1,2\n3,4" | $CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" 2>&1

# clickhouse-local executes query files before --query. clickhouse-client still rejects the combination.
echo "SELECT 'from file';" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 'from query';" < /dev/null
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 2;" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" --query "SELECT 'from long query';" < /dev/null
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" --query "SELECT 4;" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Multiple files run in order, and a SQL UDF created in a file is available to --query.
SECOND_SQL_FILE_NAME="${TEMP_SQL_FILE_NAME}.second"
printf "%s\n" "CREATE FUNCTION f_01523 AS (x) -> x + 1;" "SELECT 'first file';" > "$TEMP_SQL_FILE_NAME"
printf "%s\n" "SELECT 'second file';" > "$SECOND_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" --queries-file="$SECOND_SQL_FILE_NAME" -q "SELECT f_01523(41);" < /dev/null

# Query files and --query keep their own ordering, regardless of interleaving on the command line.
echo "SELECT 'first file';" > "$TEMP_SQL_FILE_NAME"
echo "SELECT 'second file';" > "$SECOND_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL -q "SELECT 'first query';" --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 'second query';" --queries-file="$SECOND_SQL_FILE_NAME" < /dev/null

# Redirected stdin must not become an implicit table until after query files finish.
STDIN_DATA_FILE_NAME="${TEMP_SQL_FILE_NAME}.data"
printf '1,2\n3,4\n5,6\n' > "$STDIN_DATA_FILE_NAME"
echo "SELECT count();" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" < "$STDIN_DATA_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT count();" < "$STDIN_DATA_FILE_NAME"
$CLICKHOUSE_LOCAL -q "SELECT count();" --queries-file="$TEMP_SQL_FILE_NAME" < "$STDIN_DATA_FILE_NAME"

# --copy is a format-conversion shortcut, not a second query mode for --queries-file.
$CLICKHOUSE_LOCAL --copy --queries-file="$TEMP_SQL_FILE_NAME" --input-format CSV < "$STDIN_DATA_FILE_NAME" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Errors in a query file must stop execution before --query.
echo "SELECT throwIf(1, 'file failed');" > "$TEMP_SQL_FILE_NAME"
if output=$($CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 'trailing query';" < /dev/null 2>&1); then
    echo "unexpected success"
elif [[ "$output" == *"file failed"* ]] && ! grep -Fxq 'trailing query' <<< "$output"; then
    echo "file error stopped query"
else
    echo "unexpected output"
fi

rm "$TEMP_SQL_FILE_NAME" "$SECOND_SQL_FILE_NAME" "$STDIN_DATA_FILE_NAME"
