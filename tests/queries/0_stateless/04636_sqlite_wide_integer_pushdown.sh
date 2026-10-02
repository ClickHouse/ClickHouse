#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The sink stores `Int128`/`UInt128`/`Int256`/`UInt256` (like `UInt64`) as SQLite TEXT, where SQLite compares
# values lexicographically and orders every text value after every numeric one. This test checks that
# numeric predicates on such columns of an explicit `ENGINE = SQLite` table are not pushed down to SQLite
# (which would drop the wrong rows) but applied by ClickHouse.

DB_PATH="${CLICKHOUSE_TMP}/04636_sqlite_wide_int.db"
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" 'CREATE TABLE t (i128 TEXT, u128 TEXT, i256 TEXT, u256 TEXT);'

${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (i128 Int128, u128 UInt128, i256 Int256, u256 UInt256) ENGINE = SQLite('${DB_PATH}', 't');

INSERT INTO t VALUES (10, 10, 10, 10), (2, 2, 2, 2), (-5, 5, -5, 5), (170141183460469231731687303715884105727, 340282366920938463463374607431768211455, 57896044618658097711785492504343953926634992332820282019728792003956564819967, 115792089237316195423570985008687907853269984665640564039457584007913129639935);

SELECT 'range filter i128 > 2 (text compare would order 10 before 2):';
SELECT i128 FROM t WHERE i128 > 2 ORDER BY i128;

SELECT 'range filter i128 < 0 (negative values stored as text):';
SELECT i128 FROM t WHERE i128 < 0;

SELECT 'equality i128 = 10 (text compare would miss the numeric literal):';
SELECT i128 FROM t WHERE i128 = 10;

SELECT 'range filter u128 > 2:';
SELECT u128 FROM t WHERE u128 > 2 ORDER BY u128;

SELECT 'range filter i256 > 2:';
SELECT i256 FROM t WHERE i256 > 2 ORDER BY i256;

SELECT 'range filter u256 > 2:';
SELECT u256 FROM t WHERE u256 > 2 ORDER BY u256;

-- A bare numeric literal above the unsigned 64-bit range parses as Float64, so cast the probe value through
-- a string to compare exact wide-integer values.
SELECT 'equality on the maximum u256 value:';
SELECT u256 FROM t WHERE u256 = toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935');
"

rm -f "${DB_PATH}"
