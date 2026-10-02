#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reads through the `sqlite` table function, the `SQLite` table engine and the `SQLite` database engine open the
# database file read-only, so they never modify it. A hot journal left behind by a crashed writer is not rolled
# back (and deleted) as a side effect of a `SELECT`, `DESCRIBE` or `SHOW TABLES`: the read fails instead and the
# file stays untouched. Only a write (`INSERT`) opens the file read-write and recovers the journal.

BASE="${USER_FILES_PATH}/05261_sqlite_hot_journal_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"
JOURNAL_PATH="${DB_PATH}-journal"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05261"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_05261"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

sqlite3 "${DB_PATH}" "CREATE TABLE tbl (v INTEGER, s TEXT); INSERT INTO tbl VALUES (1, 'a'), (2, 'b');"

# `CREATE` may create a missing database file, so it opens the file read-write: create the objects beforehand.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05261 (v Int64, s String) ENGINE = SQLite('${DB_PATH}', 'tbl')"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_05261 ENGINE = SQLite('${DB_PATH}')"

# Simulate a writer that crashed in the middle of a transaction: a tiny page cache makes SQLite spill modified pages
# into the database file (syncing the journal header first), then the process is killed before `COMMIT`.
(printf 'PRAGMA cache_size = 2;\nBEGIN;\nUPDATE tbl SET v = v + 10;\nINSERT INTO tbl SELECT 100, printf("%%.1000c", "x") FROM tbl, tbl, tbl, tbl, tbl, tbl, tbl;\n.shell kill -9 $PPID\n' \
    | sqlite3 "${DB_PATH}") > /dev/null 2>&1

# The server may run under another user: let it write the directory, so that a write can delete the journal.
chmod -R ugo+rwX "${BASE}"

[ -s "${JOURNAL_PATH}" ] && echo "hot journal created"

function journal_state()
{
    if [ -e "${JOURNAL_PATH}" ]; then echo "journal kept"; else echo "journal removed"; fi
}

echo "--- table function"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM sqlite('${DB_PATH}', 'tbl')" 2>&1 | grep -o -m1 'attempt to write a readonly database' | head -n1
journal_state
${CLICKHOUSE_CLIENT} --query "DESCRIBE sqlite('${DB_PATH}', 'tbl')" 2>&1 | grep -o -m1 'attempt to write a readonly database' | head -n1
journal_state

echo "--- table engine"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_05261" 2>&1 | grep -o -m1 'attempt to write a readonly database' | head -n1
journal_state

echo "--- database engine"
# Table discovery swallows the error (so that `system.tables` keeps working), but must not touch the file either.
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${CLICKHOUSE_DATABASE}_05261" > /dev/null 2>&1
journal_state
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}_05261.tbl" 2>&1 | grep -o -m1 'attempt to write a readonly database' | head -n1
journal_state

echo "--- write recovers the journal"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05261 VALUES (3, 'c')"
journal_state
${CLICKHOUSE_CLIENT} --query "SELECT v, s FROM t_05261 ORDER BY v"
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${CLICKHOUSE_DATABASE}_05261"
