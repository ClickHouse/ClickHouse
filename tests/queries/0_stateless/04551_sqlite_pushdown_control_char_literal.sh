#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

# A pushed-down filter must escape string literals the SQLite way: only the quote is doubled, every other byte
# (backslashes, control characters such as a newline or a tab) stays literal. The ClickHouse `Regular` /
# `PostgreSQL` escaping styles would emit `\n`/`\t` as two-character backslash sequences, so the comparison
# would look for different bytes and silently miss the matching row.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_pushdown_control_char.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# The stored values contain a real newline, a real tab and a backslash.
sqlite3 "$DB_PATH" "CREATE TABLE t(id INTEGER, s TEXT);"
sqlite3 "$DB_PATH" "INSERT INTO t VALUES (1, 'a
b');"
sqlite3 "$DB_PATH" "INSERT INTO t VALUES (2, 'a	b');"
sqlite3 "$DB_PATH" "INSERT INTO t VALUES (3, 'a\\b');"
sqlite3 "$DB_PATH" "INSERT INTO t VALUES (4, 'plain');"

echo 'Pushdown of a literal with a newline finds its row:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE s = 'a\nb'"

echo 'Pushdown of a literal with a tab finds its row:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE s = 'a\tb'"

echo 'Pushdown of a literal with a backslash finds its row:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE s = 'a\\\\b'"

echo 'The same pushdown through a query-backed table function:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', query('SELECT * FROM t')) WHERE s = 'a\nb'"
