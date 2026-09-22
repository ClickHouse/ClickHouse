#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The structure of a query-backed SQLite source checks a numeric declared type against the storage class of
# the first result row, because SQLite reports a declared type for a compound `SELECT` as well. A `NULL`
# first value and an empty result carry no type information, so neither contradicts the declared type: the
# column keeps it instead of being widened to `String`. A later row of another storage class is caught by
# the fail-closed read path, and the column can be read as text to accept mixed values.

DB="${CLICKHOUSE_TMP}/05238.sqlite3"
rm -f "$DB"

sqlite3 "$DB" 'CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER, ratio REAL, name TEXT);'
sqlite3 "$DB" "INSERT INTO t VALUES (1, NULL, NULL, NULL), (2, 42, 2.5, 'b'), (3, 7, 1, 'c');"

${CLICKHOUSE_LOCAL} --multiquery "
SELECT '-- a NULL first value keeps the declared numeric types, like the table itself';
DESCRIBE TABLE sqlite('${DB}', 't');
DESCRIBE TABLE sqlite('${DB}', query('SELECT score, ratio, name FROM t ORDER BY id'));
SELECT * FROM sqlite('${DB}', query('SELECT score, ratio, name FROM t ORDER BY id'));

SELECT '-- so does an empty result';
DESCRIBE TABLE sqlite('${DB}', query('SELECT score, ratio, name FROM t WHERE id > 100'));
SELECT count() FROM sqlite('${DB}', query('SELECT score, ratio, name FROM t WHERE id > 100'));

SELECT '-- an undeclared column with a NULL first value is String, there is nothing else to type it from';
DESCRIBE TABLE sqlite('${DB}', query('SELECT score + 0 AS s FROM t ORDER BY id'));
SELECT s FROM sqlite('${DB}', query('SELECT score + 0 AS s FROM t ORDER BY id'));

SELECT '-- a compound whose first row is NULL keeps the declared INTEGER; the TEXT row of the other arm then fails the read';
DESCRIBE TABLE sqlite('${DB}', query('SELECT score FROM t WHERE id = 1 UNION ALL SELECT name FROM t WHERE id = 2'));
SELECT score FROM sqlite('${DB}', query('SELECT score FROM t WHERE id = 1 UNION ALL SELECT name FROM t WHERE id = 2')); -- { serverError INCORRECT_DATA }

SELECT '-- the same values are read as text on request';
SELECT x FROM sqlite('${DB}', query('SELECT CAST(score AS TEXT) AS x FROM t WHERE id = 1 UNION ALL SELECT name FROM t WHERE id = 2'));
"

rm -f "$DB"
