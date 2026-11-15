#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: requires the SQLite library, which is not built in the fast test.

# A valid SQLite table name may contain control characters such as a newline or a tab. The structure lookup
# (`pragma_table_xinfo`) and the existence check (`sqlite_master`) must pass the name byte-faithfully - as a
# bound statement parameter - instead of re-serializing it into SQL text: SQLite string literals have no escape
# sequences, so a backslash-escaped `a\nb` would look up the four bytes `a`, `\`, `n`, `b` and miss the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CURR_DATABASE="test_04618_sqlite_${CLICKHOUSE_DATABASE}"
DB_PATH="${USER_FILES_PATH}/${CURR_DATABASE}.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS \`${CURR_DATABASE}\`"
    rm -f "${DB_PATH}"
}
trap cleanup EXIT

rm -f "${DB_PATH}"

# Table names with a real newline and a real tab.
sqlite3 "${DB_PATH}" 'CREATE TABLE "a
b"(x INTEGER, s TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO \"a
b\" VALUES (1, 'one'), (2, 'two');"
sqlite3 "${DB_PATH}" 'CREATE TABLE "a	b"(x INTEGER);'
sqlite3 "${DB_PATH}" 'INSERT INTO "a	b" VALUES (42);'

chmod ugo+w "${DB_PATH}"

echo 'Schema inference for a table name with a newline:'
${CLICKHOUSE_CLIENT} --query="DESCRIBE sqlite('${DB_PATH}', 'a\nb') SETTINGS schema_inference_make_columns_nullable = 1"

echo 'Read through the table function for a table name with a newline:'
${CLICKHOUSE_CLIENT} --query="SELECT x, s FROM sqlite('${DB_PATH}', 'a\nb') ORDER BY x"

echo 'Read through the table function for a table name with a tab:'
${CLICKHOUSE_CLIENT} --query="SELECT x FROM sqlite('${DB_PATH}', 'a\tb')"

echo 'Existence check and read through the database engine:'
${CLICKHOUSE_CLIENT} --query="CREATE DATABASE \`${CURR_DATABASE}\` ENGINE = SQLite('${DB_PATH}')"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM \`${CURR_DATABASE}\`.\`a
b\`"
${CLICKHOUSE_CLIENT} --query="EXISTS TABLE \`${CURR_DATABASE}\`.\`a\nb\`"
${CLICKHOUSE_CLIENT} --query="EXISTS TABLE \`${CURR_DATABASE}\`.\`a\tb\`"

echo 'A missing name that is a backslash-escaped rendering of the real one does not exist:'
${CLICKHOUSE_CLIENT} --query="EXISTS TABLE \`${CURR_DATABASE}\`.\`a\\\\nb\`"
