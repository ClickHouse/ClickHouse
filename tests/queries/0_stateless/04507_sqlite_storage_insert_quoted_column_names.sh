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

# Column names that require identifier quoting (a single quote, and a space). Both round-trip through the
# SQLite engine on INSERT and SELECT. Column names containing a double quote or a backslash are not covered:
# the read path quotes identifiers with ClickHouse's double-quote escaping (`"` -> `\"`, `\` -> `\\`), which
# SQLite does not understand, so such a column can be written but not read back yet.
sqlite3 "${DB_PATH}" <<'SQL'
CREATE TABLE tbl("a'b" INTEGER, "c d" INTEGER);
SQL

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04507 (\`a'b\` Nullable(Int64), \`c d\` Nullable(Int64)) ENGINE = SQLite('${DB_PATH}', 'tbl')"

echo 'Rows after INSERT into SQLite columns requiring identifier quoting:'
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04507 (\`a'b\`, \`c d\`) VALUES (1, 2)"
${CLICKHOUSE_CLIENT} --query "SELECT \`a'b\`, \`c d\` FROM t_04507 FORMAT TSVWithNames"
