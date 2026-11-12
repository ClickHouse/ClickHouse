#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/05222_sqlite_file_replacement_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"
NEW_DB_PATH="${BASE}/new.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05222"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

# The database file is reachable when the table is created, but does not contain the target table yet, so the
# generated-column classification of the explicit column list cannot be derived from the remote schema and stays
# pending. The storage keeps the connection it was created with, which is pinned to this very file.
sqlite3 "${DB_PATH}" "CREATE TABLE unrelated(x INTEGER);"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05222 (a Nullable(Int64), b Nullable(Int64)) ENGINE = SQLite('${DB_PATH}', 'tbl')"

# Replace the database file at the same path with one that contains the table, whose column `b` is generated.
# The cached connection still sees the old, unlinked file (no `tbl`); only a fresh connection sees the
# replacement. The deferred repair must probe through a fresh connection, otherwise it never completes and `b`
# stays insertable for the rest of the table's lifetime.
sqlite3 "${NEW_DB_PATH}" "CREATE TABLE tbl(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);"
sqlite3 "${NEW_DB_PATH}" "INSERT INTO tbl(a) VALUES (10);"
mv "${NEW_DB_PATH}" "${DB_PATH}"

echo 'The first query after the replacement repairs the classification, so SELECT * returns only the base column:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_05222 ORDER BY a FORMAT TSVWithNames"

echo 'Insert without a column list targets only the base column; SQLite computes the generated column:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05222 VALUES (5)"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM t_05222 ORDER BY a FORMAT TSVWithNames"

echo 'Explicitly writing into the generated column is rejected:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05222 (a, b) VALUES (7, 100)" 2>&1 | grep -oF -m1 "Cannot insert column b, because it is MATERIALIZED column"
