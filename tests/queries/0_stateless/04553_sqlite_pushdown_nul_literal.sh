#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

# A string literal with an embedded NUL byte cannot be represented in the SQL text passed to SQLite (the
# statement text is NUL-terminated, so it would be silently truncated at the embedded NUL). A filter over such
# a literal must not be pushed down: it stays local, so the query still finds the matching row instead of
# failing or comparing against a truncated literal.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_pushdown_nul.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" 'CREATE TABLE t(id INTEGER, s TEXT);'

# The write path binds values as statement parameters, so a NUL byte survives the insert.
${CLICKHOUSE_LOCAL} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 't') VALUES (1, concat('a', char(0), 'b')), (2, 'plain')"

echo 'A filter over a literal with a NUL byte is applied locally and finds its row:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE s = concat('a', char(0), 'b')"

echo 'The same filter through the SQLite table engine:'
${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE t (id Int64, s String) ENGINE = SQLite('${DB_PATH}', 't');
    SELECT id FROM t WHERE s = concat('a', char(0), 'b');
"

echo 'An IN set containing a NUL literal is applied locally too:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE s IN (concat('a', char(0), 'b'), 'plain') ORDER BY id"

echo 'A NUL-free predicate in the same query is still pushed down and correct:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE s = 'plain' AND s != concat('a', char(0), 'b')"
