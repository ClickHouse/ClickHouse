#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/04505_sqlite_dqs_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04505"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

sqlite3 "${DB_PATH}" "CREATE TABLE tbl(a INTEGER);"
sqlite3 "${DB_PATH}" "INSERT INTO tbl(a) VALUES (1), (2);"

# Declare a column `missing` that does not exist in the remote SQLite table. The storage quotes generated
# identifiers with strict backquotes, so SQLite raises "no such column" instead of reinterpreting an unresolved
# double-quoted identifier as a string literal.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04505 (a Nullable(Int64), missing Nullable(String)) ENGINE = SQLite('${DB_PATH}', 'tbl')"

echo 'Reading a column that does not exist in the remote table fails closed instead of returning silently wrong data:'
${CLICKHOUSE_CLIENT} --query "SELECT missing FROM t_04505 ORDER BY a" 2>&1 | grep -oF -m1 "no such column"

echo 'Reading the existing base column still works:'
${CLICKHOUSE_CLIENT} --query "SELECT a FROM t_04505 ORDER BY a"
