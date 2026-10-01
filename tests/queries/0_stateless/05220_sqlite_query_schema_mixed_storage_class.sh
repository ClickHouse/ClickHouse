#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A result column without a declared SQLite type (an expression) is typed from the first row, but SQLite
# may return another storage class in a later row. Such a row must fail the read instead of being silently
# coerced by the native accessor (`1.5` read as `1`, `'abc'` read as `0`).

DB="${CLICKHOUSE_TMP}/05220.sqlite3"
rm -f "$DB"

sqlite3 "$DB" 'CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);'
sqlite3 "$DB" "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');"

${CLICKHOUSE_LOCAL} --multiquery "
SELECT '-- the first row makes the column Int64, a REAL value in a later row fails the read';
DESCRIBE TABLE sqlite('${DB}', query('SELECT CASE WHEN id = 1 THEN 1 ELSE 1.5 END AS x FROM t ORDER BY id'));
SELECT x FROM sqlite('${DB}', query('SELECT CASE WHEN id = 1 THEN 1 ELSE 1.5 END AS x FROM t ORDER BY id')) ORDER BY x; -- { serverError INCORRECT_DATA }

SELECT '-- a TEXT value in an Int64 column fails as well';
SELECT x FROM sqlite('${DB}', query('SELECT CASE WHEN id = 1 THEN 1 ELSE ''abc'' END AS x FROM t ORDER BY id')) ORDER BY x; -- { serverError INCORRECT_DATA }

SELECT '-- the first row makes the column Float64, a small INTEGER value converts exactly and is read';
DESCRIBE TABLE sqlite('${DB}', query('SELECT CASE WHEN id = 1 THEN 1.5 ELSE id END AS x FROM t ORDER BY id'));
SELECT x FROM sqlite('${DB}', query('SELECT CASE WHEN id = 1 THEN 1.5 ELSE id END AS x FROM t ORDER BY id')) ORDER BY x;

SELECT '-- an INTEGER value beyond 2^53 does not convert exactly to Float64 and fails the read';
SELECT x FROM sqlite('${DB}', query('SELECT CASE WHEN id = 1 THEN 1.5 ELSE 9007199254740993 END AS x FROM t ORDER BY id')) ORDER BY x; -- { serverError INCORRECT_DATA }

SELECT '-- a TEXT value in a Float64 column fails the read';
SELECT x FROM sqlite('${DB}', query('SELECT CASE WHEN id = 1 THEN 1.5 ELSE ''1'' END AS x FROM t ORDER BY id')) ORDER BY x; -- { serverError INCORRECT_DATA }

SELECT '-- mixed values are read when the column is declared as String or cast to text in SQLite';
CREATE TABLE mixed_as_string (x Nullable(String)) ENGINE = SQLite('${DB}', query('SELECT CASE WHEN id = 1 THEN 1 ELSE 1.5 END AS x FROM t ORDER BY id'));
SELECT x FROM mixed_as_string ORDER BY x;
SELECT x, toTypeName(x) FROM sqlite('${DB}', query('SELECT CAST(CASE WHEN id = 1 THEN 1 ELSE 1.5 END AS TEXT) AS x FROM t ORDER BY id')) ORDER BY x;

SELECT '-- a declared column keeps its declared type contract and is read as before';
SELECT id, toTypeName(id) FROM sqlite('${DB}', query('SELECT id FROM t WHERE id > 1')) ORDER BY id;

SELECT '-- an explicitly declared numeric type over an undeclared column is enforced the same way';
CREATE TABLE mixed_as_int (x Nullable(Int64)) ENGINE = SQLite('${DB}', query('SELECT CASE WHEN id = 1 THEN 1 ELSE 1.5 END AS x FROM t ORDER BY id'));
SELECT x FROM mixed_as_int ORDER BY x; -- { serverError INCORRECT_DATA }
CREATE TABLE doubled_as_int (x Nullable(Int64)) ENGINE = SQLite('${DB}', query('SELECT id * 2 AS x FROM t'));
SELECT x FROM doubled_as_int ORDER BY x;
"

rm -f "$DB"
