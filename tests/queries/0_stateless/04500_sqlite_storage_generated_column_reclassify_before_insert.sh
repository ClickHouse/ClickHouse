#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/04500_sqlite_reclassify_${CLICKHOUSE_DATABASE}"
DB_DIR="${BASE}/db"
DB_PATH="${DB_DIR}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04500"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${DB_DIR}"

# A SQLite table with a generated column `b`. Its `MATERIALIZED` (readable, non-insertable) classification comes
# from the remote SQLite schema, not from the ClickHouse metadata: a `MATERIALIZED` column without a default
# expression is formatted without the `MATERIALIZED` keyword, so the stored table definition spells the
# generated column as an ordinary one and the classification has to be re-derived from the remote schema.
sqlite3 "${DB_PATH}" "CREATE TABLE tbl(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);"
sqlite3 "${DB_PATH}" "INSERT INTO tbl(a) VALUES (10);"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04500 (a Nullable(Int64), b Nullable(Int64)) ENGINE = SQLite('${DB_PATH}', 'tbl')"

# Detach the table and make its database file unreachable (remove the whole directory so opening it does not
# silently create an empty database), then re-attach while the file is unavailable: this replays the stored
# definition (`b` spelled as an ordinary column) exactly as on a server restart while the file is temporarily
# missing. The attach must succeed even though the classification cannot be re-derived yet; the storage logs a
# (non-fatal, expected) "cannot access sqlite database" error, so drop the streamed server log for this command.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_04500"
rm -rf "${DB_DIR}"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_04500" 2>/dev/null

# Make the database file reachable again. Unlike 04499, no SELECT warms the storage up first: the very first
# operation after recovery is the INSERT itself. The pending generated-column classification must therefore be
# repaired before the insert's metadata snapshot is taken (in `updateExternalDynamicMetadataIfExists`), otherwise
# the insert is planned as if `b` were an ordinary column and `INSERT INTO t VALUES (5)` would demand a value for
# it, and `SQLiteSink` would try to write into SQLite's generated column.
mkdir -p "${DB_DIR}"
sqlite3 "${DB_PATH}" "CREATE TABLE tbl(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);"
sqlite3 "${DB_PATH}" "INSERT INTO tbl(a) VALUES (10);"

echo 'The first operation after recovery is the insert; it targets only the base column and SQLite computes the generated one:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04500 VALUES (5)"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM t_04500 ORDER BY a FORMAT TSVWithNames"

echo 'SELECT * returns only the base column, confirming the generated column stays MATERIALIZED:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_04500 ORDER BY a FORMAT TSVWithNames"

echo 'Explicitly writing into the generated column is rejected:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04500 (a, b) VALUES (7, 100)" 2>&1 | grep -oF -m1 "Cannot insert column b, because it is MATERIALIZED column"
