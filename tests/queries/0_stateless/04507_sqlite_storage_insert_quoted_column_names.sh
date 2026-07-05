#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/04507_sqlite_quoted_columns_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04507"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

sqlite3 "${DB_PATH}" <<'SQL'
CREATE TABLE tbl("a'b" INTEGER, "a\b" INTEGER);
SQL

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04507 (\`a'b\` Nullable(Int64), \`a\\b\` Nullable(Int64)) ENGINE = SQLite('${DB_PATH}', 'tbl')"

echo 'Rows after INSERT into SQLite columns requiring identifier quoting:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04507 (\`a'b\`, \`a\\b\`) VALUES (1, 2)"
${CLICKHOUSE_CLIENT} --query "SELECT \`a'b\`, \`a\\b\` FROM t_04507 FORMAT TSVWithNames"
