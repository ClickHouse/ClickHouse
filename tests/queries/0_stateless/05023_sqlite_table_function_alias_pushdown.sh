#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An alias on a `sqlite` table function must not hide the source columns from `transformQueryForExternalDatabase`:
# a predicate written as `s.i = 1` has to be pushed down exactly like the unqualified `i = 1`, and it must still be
# seen by `external_table_strict_query` when it cannot be pushed down.

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_alias.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# The table is STRICT, so `i` (`Int64` over INTEGER NOT NULL) is pushdown-safe.
sqlite3 "$DB_PATH" "
CREATE TABLE t (i INTEGER NOT NULL, s TEXT NOT NULL) STRICT;
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
"

${CLICKHOUSE_LOCAL} --query="
SELECT 'Alias-qualified filter over a table function:';
SELECT s.i, s.s FROM sqlite('${DB_PATH}', 't') AS s WHERE s.i = 1;

SELECT 'Alias-qualified filter in a join:';
SELECT s.i FROM sqlite('${DB_PATH}', 't') AS s JOIN (SELECT 1 AS k) AS n ON s.i = n.k WHERE s.i = 1;
"

# The trace proves the alias-qualified predicates reach SQLite instead of degrading into a full scan.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
SELECT s.i FROM sqlite('${DB_PATH}', 't') AS s WHERE s.i = 1 FORMAT Null;
SELECT s.i FROM sqlite('${DB_PATH}', 't') AS s JOIN (SELECT 1 AS k) AS n ON s.i = n.k WHERE s.i = 1 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `i` FROM `t`( WHERE .*)?$'

# Strict mode must reject an alias-qualified filter that SQLite cannot evaluate.
${CLICKHOUSE_LOCAL} --query="
SELECT s.i FROM sqlite('${DB_PATH}', 't') AS s WHERE toString(s.i) = '1' SETTINGS external_table_strict_query = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'

# The same holds for a query-backed source, where no outer filter can be pushed down at all.
${CLICKHOUSE_LOCAL} --query="
SELECT count() FROM sqlite('${DB_PATH}', query('SELECT i FROM t')) AS s WHERE s.i = 1 SETTINGS external_table_strict_query = 1;
" 2>&1 | grep -c 'INCORRECT_QUERY'
