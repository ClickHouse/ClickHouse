#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/04506_sqlite_missing_file_${CLICKHOUSE_DATABASE}"
DB_DIR="${BASE}/db"
DB_PATH="${DB_DIR}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04506"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${DB_DIR}"

# A SQLite table with a generated column `b`. Its `MATERIALIZED` (readable, non-insertable) classification comes
# from the remote SQLite schema, not from the ClickHouse metadata (a `MATERIALIZED` column without a default
# expression is formatted without the `MATERIALIZED` keyword), so the stored table definition replayed on
# `ATTACH` spells the generated column as an ordinary one and the classification has to be re-derived.
sqlite3 "${DB_PATH}" "CREATE TABLE tbl(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);"
sqlite3 "${DB_PATH}" "INSERT INTO tbl(a) VALUES (10);"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04506 (a Nullable(Int64), b Nullable(Int64)) ENGINE = SQLite('${DB_PATH}', 'tbl')"

# Detach the table and make its whole directory unreachable, then re-attach: this replays the stored definition
# (`b` spelled as an ordinary column) while the file is unavailable, so the classification is pending.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_04506"
rm -rf "${DB_DIR}"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_04506" 2>/dev/null

# Recreate the directory but NOT the database file, then run a query. The directory is now writable, so a
# create-on-missing open would fabricate a brand-new empty database here, observe no table, and (before the fix)
# clear the pending classification permanently. Fail closed instead: the query errors because the file is still
# missing, and the classification must stay pending. (The error is expected; drop the streamed server log.)
mkdir -p "${DB_DIR}"
${CLICKHOUSE_CLIENT} --query "SELECT a FROM t_04506 ORDER BY a" > /dev/null 2>&1 || true

# Now restore the real database file with the generated column and run the first successful query, which opens
# the real database and repairs the pending classification.
sqlite3 "${DB_PATH}" "CREATE TABLE tbl(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);"
sqlite3 "${DB_PATH}" "INSERT INTO tbl(a) VALUES (10);"
${CLICKHOUSE_CLIENT} --query "SELECT a FROM t_04506 ORDER BY a" > /dev/null

echo 'After the real file is restored the classification is repaired, so SELECT * returns only the base column:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_04506 ORDER BY a FORMAT TSVWithNames"

echo 'Insert without a column list targets only the base column; SQLite computes the generated column:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04506 VALUES (5)"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM t_04506 ORDER BY a FORMAT TSVWithNames"

echo 'Explicitly writing into the generated column is rejected:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04506 (a, b) VALUES (7, 100)" 2>&1 | grep -oF -m1 "Cannot insert column b, because it is MATERIALIZED column"
