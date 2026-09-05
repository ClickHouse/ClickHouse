#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_sqlite_proxy_restore"
DB_PATH="${BASE}/data.sqlite"
DB_RELATIVE_PATH="${CLICKHOUSE_DATABASE}_sqlite_proxy_restore/data.sqlite"
TABLE_NAME="sqlite_proxy_restore"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_NAME}"
    rm -rf "$BASE"
}
trap cleanup EXIT

cleanup
mkdir -p "$BASE"

# The explicit cached structure lets the table-function proxy load while the SQLite file is missing. The failed
# read resolves its nested `StorageSQLite`, whose generated-column classification remains pending.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE_NAME} (a Int64, b Int64)
AS sqlite('${DB_RELATIVE_PATH}', 't')
"

echo 'Read while the SQLite file is missing:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${TABLE_NAME}" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'

sqlite3 "$DB_PATH" "
CREATE TABLE t (a INTEGER NOT NULL, b INTEGER GENERATED ALWAYS AS (a * 2) STORED) STRICT;
INSERT INTO t(a) VALUES (1);
"

echo 'First successful read after restoring the SQLite file:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${TABLE_NAME} ORDER BY a"

echo 'Generated-column classification copied to the proxy:'
${CLICKHOUSE_CLIENT} --query "
SELECT default_kind
FROM system.columns
WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND name = 'b'
"

# An insert without a column list gets its sample block from the proxy metadata. It must contain only `a`, while
# SQLite computes the generated column `b`.
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE_NAME} VALUES (3)"

echo 'Insert after metadata refresh:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${TABLE_NAME} ORDER BY a"
