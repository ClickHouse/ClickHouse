#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: requires the SQLite library, which is not built in the fast test.
# no-parallel: dealing with an SQLite database makes concurrent SHOW TABLES queries fail sporadically with the "database is locked" error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CURR_DATABASE="test_04494_sqlite_${CLICKHOUSE_DATABASE}"
DB_PATH="${USER_FILES_PATH}/${CURR_DATABASE}.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS ${CURR_DATABASE}"
    rm -f "${DB_PATH}"
}
trap cleanup EXIT

rm -f "${DB_PATH}"

# The `_` in the `sqlite_` prefix must be treated as a literal underscore, not a `LIKE` wildcard:
# a genuine user table named `sqliteX` must be visible through the SQLite database engine, while
# real internal `sqlite_*` tables (here `sqlite_sequence`, created and kept alive by an
# AUTOINCREMENT column) stay hidden. `sqlite_sequence` is forced to sort before `sqliteX` in
# `sqlite_master` so that a broken filter would still leave at least one visible table.
sqlite3 "${DB_PATH}" "CREATE TABLE seqdriver(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);"
sqlite3 "${DB_PATH}" "INSERT INTO seqdriver(v) VALUES ('x');"
sqlite3 "${DB_PATH}" "DROP TABLE seqdriver;"
sqlite3 "${DB_PATH}" "CREATE TABLE sqliteX(id INTEGER, name TEXT);"
sqlite3 "${DB_PATH}" "INSERT INTO sqliteX VALUES (1, 'a'), (2, 'b');"

chmod ugo+w "${DB_PATH}"

echo 'Internal sqlite_sequence sorts first in sqlite_master:'
sqlite3 "${DB_PATH}" "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY rowid;"

${CLICKHOUSE_CLIENT} --query="CREATE DATABASE ${CURR_DATABASE} ENGINE = SQLite('${DB_PATH}')"

echo 'SHOW TABLES exposes the user table sqliteX and hides internal sqlite_* tables:'
${CLICKHOUSE_CLIENT} --query="SHOW TABLES FROM ${CURR_DATABASE}"

echo 'The user table is queryable through the database engine:'
${CLICKHOUSE_CLIENT} --query="SELECT id, name FROM ${CURR_DATABASE}.sqliteX ORDER BY id"
