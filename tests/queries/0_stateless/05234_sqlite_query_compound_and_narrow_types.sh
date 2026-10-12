#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQLite reports a declared type for the result column of a compound `SELECT` as well, taken from one of its
# arms, while the rows come from all of them. That declared type is not a contract, so a query-backed read
# must not trust it, and a value that does not fit the column type exactly must fail the read instead of
# being wrapped or rounded into it.

DB="${CLICKHOUSE_TMP}/05234.sqlite3"
rm -f "$DB"

sqlite3 "$DB" 'CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val REAL);'
sqlite3 "$DB" "INSERT INTO t VALUES (1, 'a', 1.5);"

${CLICKHOUSE_LOCAL} --multiquery "
SELECT '-- a compound SELECT reports the declared type INTEGER while returning TEXT rows: typed from the row, not from the lie';
DESCRIBE TABLE sqlite('${DB}', query('SELECT name FROM t UNION ALL SELECT id FROM t'));
SELECT x FROM sqlite('${DB}', query('SELECT name AS x FROM t UNION ALL SELECT id FROM t')) ORDER BY x;

SELECT '-- the same for a REAL arm: the column is Float64, and the INTEGER rows convert exactly';
DESCRIBE TABLE sqlite('${DB}', query('SELECT val FROM t UNION ALL SELECT id FROM t'));
SELECT x FROM sqlite('${DB}', query('SELECT val AS x FROM t UNION ALL SELECT id FROM t')) ORDER BY x;

SELECT '-- an explicitly declared Int64 over such a compound fails the read instead of reading the TEXT cell as 0';
CREATE TABLE compound_as_int (x Nullable(Int64)) ENGINE = SQLite('${DB}', query('SELECT name AS x FROM t UNION ALL SELECT id FROM t'));
SELECT x FROM compound_as_int; -- { serverError INCORRECT_DATA }

SELECT '-- a value that does not fit the declared narrow integer type exactly fails the read';
CREATE TABLE narrow_uint8 (x Nullable(UInt8)) ENGINE = SQLite('${DB}', query('SELECT 300 AS x'));
SELECT x FROM narrow_uint8; -- { serverError INCORRECT_DATA }
CREATE TABLE negative_uint8 (x Nullable(UInt8)) ENGINE = SQLite('${DB}', query('SELECT -1 AS x'));
SELECT x FROM negative_uint8; -- { serverError INCORRECT_DATA }
CREATE TABLE narrow_int16 (x Nullable(Int16)) ENGINE = SQLite('${DB}', query('SELECT 32768 AS x'));
SELECT x FROM narrow_int16; -- { serverError INCORRECT_DATA }

SELECT '-- a value that does fit is read';
CREATE TABLE fitting_uint8 (x Nullable(UInt8)) ENGINE = SQLite('${DB}', query('SELECT 255 AS x'));
SELECT x FROM fitting_uint8;

SELECT '-- a REAL value that does not survive the narrowing to Float32 fails the read';
CREATE TABLE narrow_float32 (x Nullable(Float32)) ENGINE = SQLite('${DB}', query('SELECT 16777217.0 AS x'));
SELECT x FROM narrow_float32; -- { serverError INCORRECT_DATA }
CREATE TABLE narrow_float32_int (x Nullable(Float32)) ENGINE = SQLite('${DB}', query('SELECT 16777217 AS x'));
SELECT x FROM narrow_float32_int; -- { serverError INCORRECT_DATA }

SELECT '-- a REAL value that does survive it is read';
CREATE TABLE fitting_float32 (x Nullable(Float32)) ENGINE = SQLite('${DB}', query('SELECT 16777216.0 AS x'));
SELECT x FROM fitting_float32;
CREATE TABLE fitting_float32_half (x Nullable(Float32)) ENGINE = SQLite('${DB}', query('SELECT 1.5 AS x'));
SELECT x FROM fitting_float32_half;

SELECT '-- reading the same values as String always works';
SELECT x FROM sqlite('${DB}', query('SELECT CAST(300 AS TEXT) AS x'));
"

rm -f "$DB"
