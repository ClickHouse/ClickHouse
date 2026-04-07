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

# Simultaneously passing --queries-file + --query is prohibited.
echo "SELECT 1;" > "$TEMP_SQL_FILE_NAME"
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 1;" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" -q "SELECT 2;" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_LOCAL --queries-file="$TEMP_SQL_FILE_NAME" --query "SELECT 3;" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --queries-file="$TEMP_SQL_FILE_NAME" --query "SELECT 4;" 2>&1 | grep -o 'BAD_ARGUMENTS'

rm "$TEMP_SQL_FILE_NAME"
