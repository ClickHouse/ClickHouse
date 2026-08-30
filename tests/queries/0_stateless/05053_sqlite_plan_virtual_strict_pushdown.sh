#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_plan_virtual_strict.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE t(i INTEGER NOT NULL) STRICT;
INSERT INTO t VALUES (1), (2);
"

# Plan-time virtual columns belong to this source, but do not exist in SQLite. A conjunct over one stays local
# while a safe physical conjunct can still be pushed down; a disjunction containing one stays local as a whole.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
CREATE TABLE ext ENGINE = SQLite('${DB_PATH}', 't');
SELECT i FROM ext WHERE _table = 'ext' AND i = 1 FORMAT Null;
SELECT i FROM ext WHERE _table = 'ext' OR i = 2 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `i` FROM `t`( WHERE .*)?$'

echo 'Table-backed strict mode rejects _table with the analyzer:'
${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE ext ENGINE = SQLite('${DB_PATH}', 't');
SELECT i FROM ext WHERE _table = 'ext' SETTINGS external_table_strict_query = 1, enable_analyzer = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'

echo 'Table-backed strict mode rejects _database with the old analyzer:'
${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE ext ENGINE = SQLite('${DB_PATH}', 't');
SELECT i FROM ext WHERE _database = 'missing' SETTINGS external_table_strict_query = 1, enable_analyzer = 0;
" 2>&1 | grep -c 'INCORRECT_QUERY'

echo 'Query-backed strict mode rejects _table with the analyzer:'
${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE query_ext ENGINE = SQLite('${DB_PATH}', query('SELECT i FROM t'));
SELECT i FROM query_ext WHERE _table = 'query_ext' SETTINGS external_table_strict_query = 1, enable_analyzer = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'

echo 'Query-backed strict mode rejects _database with the old analyzer:'
${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE query_ext ENGINE = SQLite('${DB_PATH}', query('SELECT i FROM t'));
SELECT i FROM query_ext WHERE _database = 'missing' SETTINGS external_table_strict_query = 1, enable_analyzer = 0;
" 2>&1 | grep -c 'INCORRECT_QUERY'
