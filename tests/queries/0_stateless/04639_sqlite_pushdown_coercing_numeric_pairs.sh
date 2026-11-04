#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A STRICT table pins the storage class of every cell, but that alone does not make a numeric column
# pushdown-safe: the ClickHouse read path (`SQLiteStatementReader::insertValue`) coerces the remote value to
# the ClickHouse column type, so whenever the accessor is not exact over the whole remote domain, SQLite and
# ClickHouse evaluate the same predicate against different values and SQLite can drop rows the local
# re-filter would keep:
#   - `UInt8` over a STRICT INTEGER cell 300 reads locally as 44 (truncation), so `x = 44` must keep the
#     row, but the pushed-down `x = 44` is false against the remote 300;
#   - `Int64` over a STRICT REAL cell 1.9 reads locally as 1 (`sqlite3_column_int64` truncates), so `x = 1`
#     must keep the row;
#   - `Float64` over a STRICT INTEGER cell 2^53 + 1 reads locally as the double 2^53 (rounding), so
#     `x = 9007199254740992.` must keep the row;
#   - `Float32` over a STRICT REAL cell 2^24 + 1 reads locally as the float 2^24, so `x = 16777216.` must
#     keep the row.
# Only the exact pairs stay pushdown-eligible: `Int64` over INTEGER and `Float64` over REAL (and `String`
# over TEXT, covered by 04637). The trace log of the queries sent to SQLite proves both directions.
# The exact-pair columns are declared NOT NULL: a nullable remote column mapped to a non-Nullable local type
# keeps its predicates local (covered by 04653_sqlite_pushdown_remote_nullability).

DB_PATH="${CLICKHOUSE_TMP}/04639_sqlite_coercion.db"
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "
CREATE TABLE t (u8 INTEGER, i64r REAL, f64i INTEGER, f32r REAL, i64 INTEGER NOT NULL, f64 REAL NOT NULL) STRICT;
INSERT INTO t VALUES (300, 1.9, 9007199254740993, 16777217.0, 10, 1.5), (44, 1.0, 1, 1.0, 2, 0.5);
"

${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (u8 UInt8, i64r Int64, f64i Float64, f32r Float32, i64 Int64, f64 Float64) ENGINE = SQLite('${DB_PATH}', 't');

SELECT 'UInt8 over STRICT INTEGER holding 300 (reads as 44), u8 = 44 must keep both rows:';
SELECT u8 FROM t WHERE u8 = 44 ORDER BY u8;

SELECT 'Int64 over STRICT REAL holding 1.9 (reads as 1), i64r = 1 must keep both rows:';
SELECT i64r FROM t WHERE i64r = 1 ORDER BY i64r;

SELECT 'Float64 over STRICT INTEGER holding 2^53 + 1 (reads as 2^53), f64i = 9007199254740992. must keep the row:';
SELECT f64i FROM t WHERE f64i = 9007199254740992. ORDER BY f64i;

SELECT 'Float32 over STRICT REAL holding 2^24 + 1 (reads as 2^24), f32r = 16777216. must keep the row:';
SELECT f32r FROM t WHERE f32r = 16777216. ORDER BY f32r;

SELECT 'Int64 over STRICT INTEGER stays pushed down and correct:';
SELECT i64 FROM t WHERE i64 > 2 ORDER BY i64;

SELECT 'Float64 over STRICT REAL stays pushed down and correct:';
SELECT f64 FROM t WHERE f64 > 0.5 ORDER BY f64;
"

# The coercing pairs must not reach SQLite (no WHERE in the query sent to it), while the exact pairs
# (Int64 over INTEGER, Float64 over REAL) must retain the transformed WHERE.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
CREATE TABLE t (u8 UInt8, i64r Int64, f64i Float64, f32r Float32, i64 Int64, f64 Float64) ENGINE = SQLite('${DB_PATH}', 't');
SELECT u8 FROM t WHERE u8 = 44 FORMAT Null;
SELECT i64r FROM t WHERE i64r = 1 FORMAT Null;
SELECT f64i FROM t WHERE f64i = 9007199254740992. FORMAT Null;
SELECT f32r FROM t WHERE f32r = 16777216. FORMAT Null;
SELECT i64 FROM t WHERE i64 > 2 FORMAT Null;
SELECT f64 FROM t WHERE f64 > 0.5 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `[^`]*` FROM `t`( WHERE .*)?$'

rm -f "${DB_PATH}"
