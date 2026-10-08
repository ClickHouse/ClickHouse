#!/usr/bin/env bash
# Tags: no-fasttest
# `Merge` and `merge` read only the tables whose names match the regexp, also over an `SQLite` database, whose table
# iterator used to ignore the name filter and return every table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_merge_regexp.db"
SQLITE_DATABASE="${CLICKHOUSE_DATABASE}_sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${SQLITE_DATABASE}"
    rm -f "${DB_PATH}"
}
trap cleanup EXIT

# The file is complete before ClickHouse opens it, so nothing writes to it while other tests list its tables.
rm -f "${DB_PATH}"
sqlite3 "${DB_PATH}" 'CREATE TABLE t1 (a INTEGER); CREATE TABLE t2 (a INTEGER); CREATE TABLE t10 (a INTEGER);
    INSERT INTO t1 VALUES (1); INSERT INTO t2 VALUES (2), (2); INSERT INTO t10 VALUES (10), (10), (10);'

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${SQLITE_DATABASE} ENGINE = SQLite('${DB_PATH}')"

${CLICKHOUSE_CLIENT} --query "SELECT _table, count() FROM merge(${SQLITE_DATABASE}, '^t1\$') GROUP BY _table ORDER BY _table"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE merge_t1 (a Int32) ENGINE = Merge(${SQLITE_DATABASE}, '^t1\$')"
${CLICKHOUSE_CLIENT} --query "SELECT _table, count() FROM merge_t1 GROUP BY _table ORDER BY _table"
${CLICKHOUSE_CLIENT} --query "DROP TABLE merge_t1"
