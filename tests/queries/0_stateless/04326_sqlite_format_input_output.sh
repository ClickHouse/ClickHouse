#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the SQLite library, which is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_format.db"
rm -f "$DB_FILE"

echo "--- write a SQLite database with the SQLite output format ---"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DB_FILE}', SQLite)
    SELECT number AS id, toString(number) AS name, if(number % 2 = 0, NULL, number) AS maybe
    FROM numbers(5)"

echo "--- the produced file is a real SQLite database (read it with the sqlite3 CLI) ---"
sqlite3 "$DB_FILE" 'SELECT id, name, maybe FROM result ORDER BY id'

echo "--- read it back with the SQLite input format (round trip, including NULLs) ---"
${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${DB_FILE}', SQLite, 'id UInt64, name String, maybe Nullable(UInt64)') ORDER BY id"

rm -f "$DB_FILE"

echo "--- read a database created directly by the sqlite3 CLI, selecting a table by name ---"
sqlite3 "$DB_FILE" "CREATE TABLE animals(name TEXT, legs INTEGER); INSERT INTO animals VALUES ('cat', 4), ('spider', 8);"
sqlite3 "$DB_FILE" "CREATE TABLE colors(value TEXT); INSERT INTO colors VALUES ('red'), ('green');"

${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${DB_FILE}', SQLite, 'name String, legs UInt8') ORDER BY name
    SETTINGS input_format_sqlite_table_name = 'animals'"

echo "--- a table name with a double quote is escaped and read correctly ---"
sqlite3 "$DB_FILE" "CREATE TABLE \"a\"\"b\"(value TEXT); INSERT INTO \"a\"\"b\" VALUES ('hello');"
${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${DB_FILE}', SQLite, 'value String')
    SETTINGS input_format_sqlite_table_name = 'a\"b'"

echo "--- a structure with a wrong number of columns is rejected ---"
${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${DB_FILE}', SQLite, 'value String, extra String')
    SETTINGS input_format_sqlite_table_name = 'a\"b'" 2>&1 | grep -o "INCORRECT_NUMBER_OF_COLUMNS" | head -n 1

rm -f "$DB_FILE"

echo "--- a SQLite text value equal to the null marker is not mistaken for SQL NULL ---"
sqlite3 "$DB_FILE" "CREATE TABLE t(value TEXT); INSERT INTO t VALUES ('NULL'), (NULL);"
${CLICKHOUSE_LOCAL} --query "
    SELECT value, value IS NULL FROM file('${DB_FILE}', SQLite, 'value Nullable(String)') ORDER BY value
    SETTINGS input_format_sqlite_table_name = 't'"

echo "--- SQL NULL in a non-nullable column with input_format_null_as_default = 0 is rejected ---"
${CLICKHOUSE_LOCAL} --query "
    SELECT value FROM file('${DB_FILE}', SQLite, 'value String')
    SETTINGS input_format_sqlite_table_name = 't', input_format_null_as_default = 0" 2>&1 | grep -o "INCORRECT_DATA" | head -n 1

echo "--- SQL NULL in a non-nullable column with input_format_null_as_default = 1 becomes the default ---"
${CLICKHOUSE_LOCAL} --query "
    SELECT value FROM file('${DB_FILE}', SQLite, 'value String') ORDER BY value
    SETTINGS input_format_sqlite_table_name = 't', input_format_null_as_default = 1"

echo "--- SQL NULL with input_format_null_as_default = 1 honors the table DEFAULT expression ---"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE defaults_test (value String DEFAULT 'fallback') ENGINE = Memory;
    INSERT INTO defaults_test FROM INFILE '${DB_FILE}'
    SETTINGS input_format_sqlite_table_name = 't', input_format_null_as_default = 1 FORMAT SQLite;
    SELECT value FROM defaults_test ORDER BY value;"

echo "--- LowCardinality(Nullable) keeps SQL NULL and the textual null marker apart ---"
${CLICKHOUSE_LOCAL} --query "
    SELECT value, value IS NULL FROM file('${DB_FILE}', SQLite, 'value LowCardinality(Nullable(String))') ORDER BY value
    SETTINGS input_format_sqlite_table_name = 't'"

echo "--- SQL NULL in a LowCardinality non-nullable column with input_format_null_as_default = 0 is rejected ---"
${CLICKHOUSE_LOCAL} --query "
    SELECT value FROM file('${DB_FILE}', SQLite, 'value LowCardinality(String)')
    SETTINGS input_format_sqlite_table_name = 't', input_format_null_as_default = 0" 2>&1 | grep -o "INCORRECT_DATA" | head -n 1

echo "--- reading works with input_format_allow_seeks = 0 (the database is buffered in memory) ---"
${CLICKHOUSE_LOCAL} --query "
    SELECT value, value IS NULL FROM file('${DB_FILE}', SQLite, 'value Nullable(String)') ORDER BY value
    SETTINGS input_format_sqlite_table_name = 't', input_format_allow_seeks = 0"

rm -f "$DB_FILE"

echo "--- writing to a non-file is not supported ---"
${CLICKHOUSE_LOCAL} --query "SELECT 1 AS x FORMAT SQLite" 2>&1 | grep -o "NOT_IMPLEMENTED" | head -n 1

rm -f "$DB_FILE"
