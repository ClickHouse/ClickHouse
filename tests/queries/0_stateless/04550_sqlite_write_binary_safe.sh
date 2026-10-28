#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

# Writes to SQLite must be byte-faithful. SQLite string literals have no escape sequences at all, so the sink
# binds each value as a statement parameter instead of formatting it into the SQL text. This verifies that a
# single quote, a backslash, control characters and an embedded NUL byte all survive a write unchanged - both
# through the `SQLite` table engine and the `sqlite` table function.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_write_binary_safe.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" 'CREATE TABLE t(c0 TEXT);'
sqlite3 "$DB_PATH" 'CREATE TABLE tf(c0 TEXT);'

echo 'Insert quote / backslash / newline / tab / NUL through the SQLite table engine:'
${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE t (c0 String) ENGINE = SQLite('${DB_PATH}', 't');
    INSERT INTO t VALUES ('a''b'), ('a\\\\b'), ('a\nb'), ('a\tb'), (concat('a', char(0), 'b'));
"

echo 'Exact bytes stored in SQLite (hex):'
sqlite3 "$DB_PATH" "SELECT hex(c0) FROM t ORDER BY hex(c0);"

echo 'Read back through ClickHouse (hex of each value, must match the bytes above):'
${CLICKHOUSE_LOCAL} --query="SELECT hex(c0) FROM sqlite('${DB_PATH}', 't') ORDER BY hex(c0)"

echo 'Insert the same values through the sqlite table function:'
${CLICKHOUSE_LOCAL} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'tf') VALUES ('a''b'), ('a\\\\b'), ('a\nb'), ('a\tb'), (concat('a', char(0), 'b'))"

echo 'Exact bytes stored by the table function (hex):'
sqlite3 "$DB_PATH" "SELECT hex(c0) FROM tf ORDER BY hex(c0);"
