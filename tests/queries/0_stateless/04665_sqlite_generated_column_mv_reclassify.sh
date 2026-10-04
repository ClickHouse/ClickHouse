#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/04665_sqlite_mv_reclassify_${CLICKHOUSE_DATABASE}"
DB_DIR="${BASE}/db"
DB_PATH="${DB_DIR}/data.sqlite"
SOURCE="source_04665"
TARGET="target_04665"
VIEW="mv_04665"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${VIEW}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${SOURCE}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TARGET}"
    rm -rf "${BASE}"
}
trap cleanup EXIT

cleanup
mkdir -p "${DB_DIR}"

# Persist an explicit ClickHouse definition for a SQLite table whose generated column `b` is classified as
# expressionless `MATERIALIZED` while the remote schema is reachable.
sqlite3 "${DB_PATH}" "CREATE TABLE tbl(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);"
${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE ${TARGET} (a Nullable(Int64), b Nullable(Int64)) ENGINE = SQLite('${DB_PATH}', 'tbl')"

# Replay the stored definition while the SQLite file is unavailable. The persisted definition spells `b` as
# an ordinary column, so its generated-column classification remains pending until the remote schema returns.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TARGET}"
rm -rf "${DB_DIR}"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TARGET}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${SOURCE} (a Nullable(Int64)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query \
    "CREATE MATERIALIZED VIEW ${VIEW} TO ${TARGET} AS SELECT a FROM ${SOURCE}"

# Restore the remote table and make the first access to the SQLite target arrive through the materialized
# view. `InsertDependenciesBuilder` must refresh the target metadata before freezing its output header;
# otherwise `SQLiteSink` receives the stale snapshot and tries to write into generated column `b`.
mkdir -p "${DB_DIR}"
sqlite3 "${DB_PATH}" "CREATE TABLE tbl(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);"

echo 'The first access after recovery is a materialized-view target write; SQLite computes the generated column:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${SOURCE} VALUES (5)"
sqlite3 "${DB_PATH}" "SELECT a, b FROM tbl ORDER BY a;"
