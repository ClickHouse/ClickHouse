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
sqlite3 "$DB_FILE" 'CREATE TABLE animals(name TEXT, legs INTEGER); INSERT INTO animals VALUES (''cat'', 4), (''spider'', 8);'
sqlite3 "$DB_FILE" 'CREATE TABLE colors(value TEXT); INSERT INTO colors VALUES (''red''), (''green'');'

${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${DB_FILE}', SQLite, 'name String, legs UInt8') ORDER BY name
    SETTINGS input_format_sqlite_table_name = 'animals'"

echo "--- writing to a non-file (a pipe) is not supported ---"
${CLICKHOUSE_LOCAL} --query "SELECT 1 AS x FORMAT SQLite" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"

rm -f "$DB_FILE"
