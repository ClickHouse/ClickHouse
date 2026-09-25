#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/04508_sqlite_missing_table_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04508"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

# Create a reachable SQLite database file without the remote table. The explicit ClickHouse column list includes
# the generated column as an ordinary column, but the remote schema cannot be observed yet. The generated-column
# classification must therefore stay pending even though the SQLite database opened successfully.
sqlite3 "${DB_PATH}" "PRAGMA user_version = 1;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04508 (a Nullable(Int64), b Nullable(Int64)) ENGINE = SQLite('${DB_PATH}', 'tbl')"

sqlite3 "${DB_PATH}" "CREATE TABLE tbl(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);"
sqlite3 "${DB_PATH}" "INSERT INTO tbl(a) VALUES (10);"

echo 'After the remote table appears the generated column classification is repaired:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_04508 ORDER BY a FORMAT TSVWithNames"

echo 'Insert without a column list targets only the base column; SQLite computes the generated column:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04508 VALUES (5)"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM t_04508 ORDER BY a FORMAT TSVWithNames"

echo 'Explicitly writing into the generated column is rejected:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04508 (a, b) VALUES (7, 100)" 2>&1 | grep -oF -m1 "Cannot insert column b, because it is MATERIALIZED column"
