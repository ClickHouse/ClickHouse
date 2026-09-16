#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

# A scalar `IN (...)` set is represented as a Tuple literal, and a `(a, b) IN ((...), (...))` set as a Tuple of
# Tuples. When such a predicate is pushed down to SQLite it must escape the string elements the SQLite way -
# only the quote is doubled, every other byte (backslashes, control characters such as a newline or a tab)
# stays literal. If the composite literal fell back to the `Regular` escaping the nested strings would be
# emitted with `\n`/`\t`/`\\` sequences, so the comparison would look for different bytes and silently miss the
# matching row - a wrong-results bug on a valid filter. This guards the composite path that the scalar test
# 04551_sqlite_pushdown_control_char_literal does not exercise.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_pushdown_in_control_char.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# The stored values contain a real newline, a real tab and a backslash.
sqlite3 "$DB_PATH" "CREATE TABLE t(id INTEGER, s TEXT);"
sqlite3 "$DB_PATH" "INSERT INTO t VALUES (1, 'a
b');"
sqlite3 "$DB_PATH" "INSERT INTO t VALUES (2, 'a	b');"
sqlite3 "$DB_PATH" "INSERT INTO t VALUES (3, 'a\\b');"
sqlite3 "$DB_PATH" "INSERT INTO t VALUES (4, 'plain');"

echo 'Pushdown of an IN set with newline, tab and backslash strings finds the rows:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE s IN ('a\nb', 'a\tb', 'a\\\\b') ORDER BY id"

echo 'Pushdown of a row IN set (tuple of tuples) with a control-char string finds its row:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE (id, s) IN ((1, 'a\nb'), (99, 'plain')) ORDER BY id"

echo 'A plain string element in the same IN set still matches:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE s IN ('plain', 'a\nb') ORDER BY id"

echo 'The same IN pushdown through a query-backed table function:'
${CLICKHOUSE_LOCAL} --query="SELECT id FROM sqlite('${DB_PATH}', query('SELECT * FROM t')) WHERE s IN ('a\nb', 'a\tb') ORDER BY id"
