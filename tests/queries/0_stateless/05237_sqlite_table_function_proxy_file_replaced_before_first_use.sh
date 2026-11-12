#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/05237_sqlite_proxy_replacement_${CLICKHOUSE_DATABASE}"
DB_RELATIVE_DIR="05237_sqlite_proxy_replacement_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05237_select_first"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05237_insert_first"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

# `CREATE TABLE ... AS sqlite(...)` without a column list infers the structure from the database file at
# creation time and wraps the table function into a proxy, whose nested storage is only instantiated on the first
# real use. The structure inference must not leave a connection behind in the table function: the proxy holds the
# table function until that first use, so such a connection would pin the file the database had at creation
# time, and the nested storage would then be classified against that stale schema instead of the current file.
#
# Here `b` is an ordinary column when the table is created, and becomes a generated column in the replacement
# file that is moved over the same path before the first use of the proxy. The first use must observe the
# replacement: `SELECT *` returns only the base column and an insert without a column list writes only `a`.
for suffix in select_first insert_first
do
    sqlite3 "${BASE}/${suffix}.sqlite" "CREATE TABLE tbl(a INTEGER, b INTEGER); INSERT INTO tbl VALUES (1, 2);"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05237_${suffix} AS sqlite('${DB_RELATIVE_DIR}/${suffix}.sqlite', 'tbl')"

    sqlite3 "${BASE}/${suffix}.new.sqlite" "CREATE TABLE tbl(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED); INSERT INTO tbl(a) VALUES (10);"
    mv "${BASE}/${suffix}.new.sqlite" "${BASE}/${suffix}.sqlite"
done

echo 'The first use is a SELECT: only the base column is returned, the generated column is not expanded:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_05237_select_first ORDER BY a FORMAT TSVWithNames"

echo 'The generated column is still readable when named explicitly:'
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM t_05237_select_first ORDER BY a"

echo 'Explicitly writing into the generated column is rejected:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05237_select_first (a, b) VALUES (7, 100)" 2>&1 | grep -oF -m1 "Cannot insert column b, because it is MATERIALIZED column"

echo 'The first use is an INSERT without a column list: only the base column is written, SQLite computes the generated column:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05237_insert_first VALUES (5)"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM t_05237_insert_first ORDER BY a FORMAT TSVWithNames"
