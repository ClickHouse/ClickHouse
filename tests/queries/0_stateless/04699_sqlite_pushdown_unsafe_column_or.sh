#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A disjunction combining a pushdown-safe column with a column excluded from pushdown must stay local as a
# whole. Removing only the unsafe branch would narrow the remote filter: a row matching only that branch is
# dropped by SQLite and never reaches the local re-filtering.
#
# The table is STRICT, so `i` (`Int64` over INTEGER NOT NULL) is pushdown-safe, while `u` (`UInt8`) is not:
# its read accessor truncates the INTEGER cell 300 to 44, so `u = 44` matches locally but a pushed-down
# `u = 44` is false remotely. `WHERE u = 44 OR i = 2` must therefore not push down `i = 2` alone - the
# (300, 1) row matches only via the coerced `u` branch.

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_unsafe_or.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE t (u INTEGER NOT NULL, i INTEGER NOT NULL) STRICT;
INSERT INTO t VALUES (300, 1), (5, 2);
"

${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (u UInt8, i Int64) ENGINE = SQLite('${DB_PATH}', 't');

SELECT 'OR of an unsafe and a safe branch, both rows must survive:';
SELECT u, i FROM t WHERE u = 44 OR i = 2 ORDER BY i;

SELECT 'AND of an unsafe and a safe conjunct, the safe one may be pushed down:';
SELECT u, i FROM t WHERE u = 44 AND i = 1;

SELECT 'OR nested under AND keeps the whole disjunction local:';
SELECT u, i FROM t WHERE (u = 44 OR i = 2) AND i >= 1 ORDER BY i;
"

# The trace of the queries sent to SQLite proves what was pushed down: no WHERE at all for the disjunction
# with an unsafe branch, only the safe conjunct(s) otherwise.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
CREATE TABLE t (u UInt8, i Int64) ENGINE = SQLite('${DB_PATH}', 't');
SELECT u, i FROM t WHERE u = 44 OR i = 2 FORMAT Null;
SELECT u, i FROM t WHERE u = 44 AND i = 1 FORMAT Null;
SELECT u, i FROM t WHERE (u = 44 OR i = 2) AND i >= 1 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `[^`]*`, `[^`]*` FROM `t`( WHERE .*)?$'

# Strict mode must reject a query whose filter cannot be fully evaluated by SQLite - both when the whole
# disjunction stays local and when only a conjunct does.
${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (u UInt8, i Int64) ENGINE = SQLite('${DB_PATH}', 't');
SET external_table_strict_query = 1;
SELECT u, i FROM t WHERE u = 44 OR i = 2;
" 2>&1 | grep -c 'INCORRECT_QUERY'

${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (u UInt8, i Int64) ENGINE = SQLite('${DB_PATH}', 't');
SET external_table_strict_query = 1;
SELECT u, i FROM t WHERE u = 44 AND i = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'

# A filter over pushdown-safe columns only is unaffected by strict mode and is pushed down entirely.
${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (u UInt8, i Int64) ENGINE = SQLite('${DB_PATH}', 't');
SET external_table_strict_query = 1;
SELECT i FROM t WHERE i = 2;
"
