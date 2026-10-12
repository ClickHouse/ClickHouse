-- `s = toFixedString('V0', 3)` compares zero-padded, so it matches the `String` values `'V0'`, `'V0\0'` and `'V0\0\0'`.
-- They sort differently, so read-in-order must not treat `s` as fixed and skip sorting on it.

SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_rio_string;
CREATE TABLE t_rio_string (k UInt8, s String) ENGINE = MergeTree ORDER BY (k, s);
INSERT INTO t_rio_string VALUES (1, 'V0\0\0'), (1, 'V0'), (1, 'V0\0'), (2, 'X');

SELECT 'String';
SELECT k, hex(s) FROM t_rio_string WHERE s = toFixedString('V0', 3) ORDER BY k, s DESC;
SELECT hex(s) FROM t_rio_string WHERE s = toFixedString('V0', 3) ORDER BY s DESC;
SELECT hex(s) FROM t_rio_string WHERE k = 1 AND s = toFixedString('V0', 3) ORDER BY k, s DESC LIMIT 1;
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT k, s FROM t_rio_string WHERE s = toFixedString('V0', 3) ORDER BY k, s DESC) WHERE explain LIKE '%Prefix sort description%';

-- A `String` constant still fixes a `String` column.
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT k, s FROM t_rio_string WHERE s = 'V0' ORDER BY k, s DESC) WHERE explain LIKE '%Prefix sort description%';

-- The same with a `Nullable` or `LowCardinality` `FixedString` constant.
SELECT 'String vs Nullable(FixedString)';
SELECT k, hex(s) FROM t_rio_string WHERE s = toNullable(toFixedString('V0', 3)) ORDER BY k, s DESC;
SELECT 'String vs LowCardinality(FixedString)';
SELECT k, hex(s) FROM t_rio_string WHERE s = toLowCardinality(toFixedString('V0', 3)) ORDER BY k, s DESC;

DROP TABLE t_rio_string;

-- `Tuple` values are compared element by element, so `t = tuple(toFixedString('V0', 3))` matches
-- `tuple('V0')`, `tuple('V0\0')` and `tuple('V0\0\0')`.
SELECT 'Tuple(String)';
DROP TABLE IF EXISTS t_rio_tuple;
CREATE TABLE t_rio_tuple (k UInt8, t Tuple(String)) ENGINE = MergeTree ORDER BY (k, t);
INSERT INTO t_rio_tuple VALUES (1, tuple('V0\0\0')), (1, tuple('V0')), (1, tuple('V0\0')), (2, tuple('X'));
SELECT k, hex(t.1) FROM t_rio_tuple WHERE t = tuple(toFixedString('V0', 3)) ORDER BY k, t DESC;
DROP TABLE t_rio_tuple;

SELECT 'Nullable(String)';
DROP TABLE IF EXISTS t_rio_nullable;
CREATE TABLE t_rio_nullable (k UInt8, s Nullable(String)) ENGINE = MergeTree ORDER BY (k, s) SETTINGS allow_nullable_key = 1;
INSERT INTO t_rio_nullable VALUES (1, 'V0\0\0'), (1, 'V0'), (1, 'V0\0'), (2, 'X');
SELECT k, hex(s) FROM t_rio_nullable WHERE s = toFixedString('V0', 3) ORDER BY k, s DESC;
DROP TABLE t_rio_nullable;

SELECT 'LowCardinality(String)';
DROP TABLE IF EXISTS t_rio_lc;
CREATE TABLE t_rio_lc (k UInt8, s LowCardinality(String)) ENGINE = MergeTree ORDER BY (k, s);
INSERT INTO t_rio_lc VALUES (1, 'V0\0\0'), (1, 'V0'), (1, 'V0\0'), (2, 'X');
SELECT k, hex(s) FROM t_rio_lc WHERE s = toFixedString('V0', 3) ORDER BY k, s DESC;
DROP TABLE t_rio_lc;

-- All values of a `FixedString` column have the same length, so at most one of them matches and the column stays fixed.
SELECT 'FixedString';
DROP TABLE IF EXISTS t_rio_fixed;
CREATE TABLE t_rio_fixed (k UInt8, s FixedString(3)) ENGINE = MergeTree ORDER BY (k, s);
INSERT INTO t_rio_fixed VALUES (1, 'V0'), (1, 'V1'), (2, 'X');
SELECT k, hex(s) FROM t_rio_fixed WHERE s = 'V0' ORDER BY k, s DESC;
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT k, s FROM t_rio_fixed WHERE s = 'V0' ORDER BY k, s DESC) WHERE explain LIKE '%Prefix sort description%';
DROP TABLE t_rio_fixed;
