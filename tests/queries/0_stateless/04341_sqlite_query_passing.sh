#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_TMP}/04341.sqlite3"
rm -f "$DB"

sqlite3 "$DB" 'CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, val REAL);'
sqlite3 "$DB" "INSERT INTO t1 VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'c', 3.5), (4, 'd', 4.5);"
sqlite3 "$DB" 'CREATE TABLE t2(id INTEGER, category TEXT);'
sqlite3 "$DB" "INSERT INTO t2 VALUES (1, 'x'), (2, 'y'), (4, 'z');"

${CLICKHOUSE_LOCAL} --multiquery "
SELECT '-- baseline: table name still works';
SELECT count() FROM sqlite('${DB}', 't1');

SELECT '-- query(...) form: filtering is done on the SQLite side';
SELECT count() FROM sqlite('${DB}', query('SELECT * FROM t1 WHERE id % 2 = 0'));

SELECT '-- subquery form';
SELECT id, name FROM sqlite('${DB}', (SELECT id, name FROM t1 WHERE id > 2)) ORDER BY id;

SELECT '-- schema inference from a passthrough query';
DESCRIBE TABLE sqlite('${DB}', query('SELECT id, name, val FROM t1'));

SELECT '-- a JOIN passed to SQLite as is';
SELECT id, name, category
FROM sqlite('${DB}', query('SELECT t1.id AS id, t1.name AS name, t2.category AS category FROM t1 JOIN t2 ON t1.id = t2.id'))
ORDER BY id;

SELECT '-- engine with a query, reading and write rejection';
CREATE TABLE engine_query ENGINE = SQLite('${DB}', query('SELECT id, name FROM t1 WHERE id <= 2'));
SELECT * FROM engine_query ORDER BY id;
INSERT INTO engine_query VALUES (5, 'e'); -- { serverError INCORRECT_QUERY }
DROP TABLE engine_query;
"

rm -f "$DB"
