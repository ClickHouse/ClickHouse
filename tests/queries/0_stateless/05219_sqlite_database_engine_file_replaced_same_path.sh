#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `SQLite` database engine must describe the database file that is at the path now. A long-lived
# connection keeps the file it was opened on: when the database file is replaced at the same path (an atomic
# `mv` of a freshly built file over it), such a handle still sees the old, unlinked file, so table discovery,
# existence checks and schema fetches would keep describing the old database indefinitely.

BASE="${USER_FILES_PATH}/05219_sqlite_db_replaced_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"
DB="db_05219_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

sqlite3 "${DB_PATH}" "
CREATE TABLE old_t (id INTEGER NOT NULL);
INSERT INTO old_t VALUES (1);
CREATE TABLE t (a INTEGER NOT NULL);
INSERT INTO t VALUES (10), (20);
"

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB} ENGINE = SQLite('${DB_PATH}')"

function show_state()
{
    echo 'tables:'
    ${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${DB}"
    echo 'system.tables:'
    ${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = '${DB}' ORDER BY name"
    echo 'exists old_t, new_t:'
    ${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.old_t"
    ${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.new_t"
    echo 'schema of t:'
    ${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${DB}.t"
    echo 'rows of t:'
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${DB}.t ORDER BY a"
}

echo '-- original file'
show_state

# Replace the database file at the same path after the database engine has already opened it.
sqlite3 "${BASE}/new.sqlite" "
CREATE TABLE new_t (id INTEGER NOT NULL);
INSERT INTO new_t VALUES (2);
CREATE TABLE t (a INTEGER NOT NULL, b TEXT);
INSERT INTO t VALUES (30, 'x'), (40, 'y');
"
mv "${BASE}/new.sqlite" "${DB_PATH}"

echo '-- replaced file'
show_state
echo 'a table of the old file is gone:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${DB}.old_t" 2>&1 | grep -oE 'UNKNOWN_TABLE' | head -1
echo 'a table of the new file is read:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${DB}.new_t"
