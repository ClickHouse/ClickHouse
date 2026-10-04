#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_dqs_compatibility.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# SQLite's default DQS behavior is part of the semantics of existing schema SQL and user-provided queries.
# ClickHouse-generated identifiers use strict backquotes instead: this preserves DQS here while making an
# unresolved generated projection fail with `no such column` instead of becoming a string literal.
sqlite3 "$DB_PATH" <<'SQL'
CREATE TABLE t(a INTEGER);
INSERT INTO t VALUES (1), (2);
CREATE VIEW v AS SELECT "hello" AS s;
CREATE TABLE weird(
    `a``b` TEXT,
    `c\d` TEXT,
    `line
break` TEXT
);
INSERT INTO weird VALUES ('one', 'two', 'three');
SQL

${CLICKHOUSE_LOCAL} --multiquery --query="
SELECT 'A stored view using a double-quoted string keeps SQLite semantics:';
SELECT s FROM sqlite('${DB_PATH}', 'v');

SELECT 'A user query using a double-quoted string keeps SQLite semantics:';
SELECT s FROM sqlite('${DB_PATH}', query('SELECT \"hello\" AS s'));

SELECT 'The same view works through the table engine:';
CREATE TABLE sqlite_view ENGINE = SQLite('${DB_PATH}', 'v');
SELECT s FROM sqlite_view;

SELECT 'Strict backquotes round-trip special identifier bytes through a table-backed source:';
SELECT * FROM sqlite('${DB_PATH}', 'weird');

SELECT 'Strict backquotes round-trip special identifier bytes through a query-backed source:';
SELECT * FROM sqlite('${DB_PATH}', query('SELECT * FROM weird'));
"

echo 'The SQLite format reader also preserves stored-view DQS semantics:'
${CLICKHOUSE_LOCAL} \
    --input-format SQLite \
    --input_format_sqlite_table_name v \
    --structure 's String' \
    --query 'SELECT s FROM table' < "$DB_PATH"

echo 'A missing table column still fails closed:'
${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE missing_column (missing String) ENGINE = SQLite('${DB_PATH}', 't');
SELECT missing FROM missing_column;
" 2>&1 | grep -oF -m1 'no such column: missing'

echo 'A missing query-result column still fails closed:'
${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE missing_query_column (a Int64, missing String) ENGINE = SQLite('${DB_PATH}', query('SELECT a FROM t'));
SELECT missing FROM missing_query_column;
" 2>&1 | grep -oF -m1 'no such column: missing'
