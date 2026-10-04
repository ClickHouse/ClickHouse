#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_identifier_quoting.db
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# Column names containing a double quote and a backslash. In SQLite an embedded double quote inside a
# quoted identifier is doubled, and a backslash is a literal character with no special meaning.
sqlite3 "$DB_PATH" 'CREATE TABLE weird("a""b" INTEGER, "c\d" TEXT);'
sqlite3 "$DB_PATH" "INSERT INTO weird VALUES (1, 'x\\y');"
sqlite3 "$DB_PATH" "INSERT INTO weird VALUES (2, 'o''quote');"

echo 'Read through the table function with schema inference:'
${CLICKHOUSE_LOCAL} --query="SELECT * FROM sqlite('${DB_PATH}', 'weird') ORDER BY 1 FORMAT TSVWithNames"

echo 'Projection of the column with a double quote in its name:'
${CLICKHOUSE_LOCAL} --query="SELECT \`a\"b\` FROM sqlite('${DB_PATH}', 'weird') ORDER BY 1"

echo 'WHERE pushdown over quoted identifiers, with a backslash inside a string literal:'
${CLICKHOUSE_LOCAL} --query="SELECT \`a\"b\` FROM sqlite('${DB_PATH}', 'weird') WHERE \`c\\\\d\` = 'x\\\\y'"

echo 'WHERE pushdown with a single quote inside a string literal:'
${CLICKHOUSE_LOCAL} --query="SELECT \`a\"b\` FROM sqlite('${DB_PATH}', 'weird') WHERE \`c\\\\d\` = 'o''quote'"

echo 'Insert through the table engine and read the row back:'
${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE sqlite_weird ENGINE = SQLite('${DB_PATH}', 'weird');
    INSERT INTO sqlite_weird VALUES (3, 'back\\\\slash');
    SELECT * FROM sqlite_weird WHERE \`a\"b\` = 3;
"

echo 'Read through a query-backed table function:'
${CLICKHOUSE_LOCAL} --query="SELECT \`a\"b\` FROM sqlite('${DB_PATH}', query('SELECT * FROM weird')) ORDER BY 1"

echo 'Read through a subquery formatted back into SQLite SQL:'
${CLICKHOUSE_LOCAL} --query="SELECT * FROM sqlite('${DB_PATH}', (SELECT \`a\"b\`, \`c\\\\d\` FROM weird)) ORDER BY 1 FORMAT TSVWithNames"
