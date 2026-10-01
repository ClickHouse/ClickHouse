#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Query-backed schema inference: SQLite reports a declared type only for a result column that is a direct
# column of a table. An expression, a literal or an aggregate has none, so its type is derived from the
# storage class of its value in the first result row instead of falling back to a string.

DB="${CLICKHOUSE_TMP}/05218.sqlite3"
rm -f "$DB"

sqlite3 "$DB" 'CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val REAL);'
sqlite3 "$DB" "INSERT INTO t VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'c', 3.5);"

${CLICKHOUSE_LOCAL} --multiquery "
SELECT '-- declared columns keep their declared types, expressions are typed from the first row';
DESCRIBE TABLE sqlite('${DB}', query('SELECT id, id + 1 AS x, CAST(id AS REAL) AS f, name || ''!'' AS s, 1 AS one, 0.5 AS half, x''00ff'' AS b, NULL AS n FROM t'));

SELECT '-- aggregates';
DESCRIBE TABLE sqlite('${DB}', query('SELECT count(*) AS c, sum(val) AS s, max(name) AS m FROM t'));

SELECT '-- an empty result has no row to type an expression from';
DESCRIBE TABLE sqlite('${DB}', query('SELECT id, id + 1 AS x FROM t WHERE id > 100'));

SELECT '-- the values are read with the inferred types';
SELECT x, f, s, one, half, toTypeName(x), toTypeName(f) FROM sqlite('${DB}', query('SELECT id + 1 AS x, CAST(id AS REAL) AS f, name || ''!'' AS s, 1 AS one, 0.5 AS half FROM t')) ORDER BY x;
SELECT c, s, m, toTypeName(c), toTypeName(s) FROM sqlite('${DB}', query('SELECT count(*) AS c, sum(val) AS s, max(name) AS m FROM t'));
SELECT sum(x) FROM sqlite('${DB}', query('SELECT id * 2 AS x FROM t'));

SELECT '-- the subquery form and the table engine infer the same way';
DESCRIBE TABLE sqlite('${DB}', (SELECT id + 1 AS x, count() AS c FROM t GROUP BY id + 1));
CREATE TABLE engine_expr ENGINE = SQLite('${DB}', query('SELECT id * 10 AS x, val / 2 AS h FROM t'));
DESCRIBE TABLE engine_expr;
SELECT * FROM engine_expr ORDER BY x;
"

rm -f "$DB"
