-- `s = toFixedString('V0', 3)` compares zero-padded, so it matches the `String` values `'V0'`, `'V0\0'` and `'V0\0\0'`.
-- They sort differently, so read-in-order must not treat `s` as fixed and skip sorting on it.
-- The same holds when the `FixedString` is held by a `Variant` or `Dynamic` constant.

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

-- A `FixedString` held by a `Variant` or `Dynamic` constant.
SELECT 'String vs Dynamic(max_types = 0) holding FixedString';
SELECT k, hex(s) FROM t_rio_string WHERE s = CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0)) ORDER BY k, s DESC;
SELECT 'String vs Dynamic holding LowCardinality(FixedString)';
SELECT k, hex(s) FROM t_rio_string WHERE s = CAST(CAST(toFixedString('V0', 3) AS LowCardinality(FixedString(3))) AS Dynamic) ORDER BY k, s DESC;
SELECT 'String vs JSONExtract to Variant(FixedString)';
SELECT k, hex(s) FROM t_rio_string WHERE s = JSONExtract('{"a":"V0"}', 'a', 'Variant(FixedString(3), UInt64)') ORDER BY k, s DESC;
SELECT 'additional_table_filters';
SELECT k, hex(s) FROM t_rio_string ORDER BY k, s DESC
SETTINGS additional_table_filters = {'t_rio_string': 'tuple(s) = tuple(CAST(toFixedString(\'V0\', 3) AS Dynamic))'};
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT k, s FROM t_rio_string WHERE s = CAST(toFixedString('V0', 3) AS Dynamic) ORDER BY k, s DESC) WHERE explain LIKE '%Prefix sort description%';

-- A `Variant` or `Dynamic` constant that holds a `String` matches one value, so `s` stays fixed.
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT k, s FROM t_rio_string WHERE s = CAST('V0' AS Dynamic) ORDER BY k, s DESC) WHERE explain LIKE '%Prefix sort description%';
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT k, s FROM t_rio_string WHERE s = CAST('V0' AS Variant(String, UInt64)) ORDER BY k, s DESC) WHERE explain LIKE '%Prefix sort description%';

-- A `Variant` or `Dynamic` expression on the column side compares zero-padded with a `FixedString` constant as well.
SELECT 'CAST(s AS Dynamic) vs FixedString';
SELECT k, hex(s) FROM t_rio_string WHERE CAST(s AS Dynamic) = toFixedString('V0', 3) ORDER BY k, CAST(s AS Dynamic) DESC
SETTINGS allow_suspicious_types_in_order_by = 1;

DROP TABLE t_rio_string;

-- With the key (s, k), a fixed `s` lets `GROUP BY k`, `LIMIT BY k` and `ORDER BY k` use the order of the key.
DROP TABLE IF EXISTS t_rio_string_first;
CREATE TABLE t_rio_string_first (s String, k UInt8) ENGINE = MergeTree ORDER BY (s, k);
INSERT INTO t_rio_string_first VALUES ('V0', 1), ('V0', 2), ('V0\0', 1), ('V0\0', 2), ('V0\0\0', 1), ('V0\0\0', 2);

SELECT 'GROUP BY in order';
SELECT k, count() FROM t_rio_string_first WHERE s = CAST(toFixedString('V0', 3) AS Dynamic) GROUP BY k ORDER BY k
SETTINGS optimize_aggregation_in_order = 1;
SELECT 'LIMIT BY';
SELECT k FROM (SELECT k FROM t_rio_string_first WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)) LIMIT 1 BY k) ORDER BY k;
SELECT 'ORDER BY k';
SELECT k FROM t_rio_string_first WHERE s = CAST(toFixedString('V0', 3) AS Dynamic) ORDER BY k;

DROP TABLE t_rio_string_first;

-- `Tuple` values are compared element by element, so `t = tuple(toFixedString('V0', 3))` matches
-- `tuple('V0')`, `tuple('V0\0')` and `tuple('V0\0\0')`, also when the element or the whole tuple is held by a `Dynamic`.
SELECT 'Tuple(String)';
DROP TABLE IF EXISTS t_rio_tuple;
CREATE TABLE t_rio_tuple (k UInt8, t Tuple(String)) ENGINE = MergeTree ORDER BY (k, t);
INSERT INTO t_rio_tuple VALUES (1, tuple('V0\0\0')), (1, tuple('V0')), (1, tuple('V0\0')), (2, tuple('X'));
SELECT k, hex(t.1) FROM t_rio_tuple WHERE t = tuple(toFixedString('V0', 3)) ORDER BY k, t DESC;
SELECT 'Tuple element held by Dynamic';
SELECT k, hex(t.1) FROM t_rio_tuple WHERE t = tuple(CAST(toFixedString('V0', 3) AS Dynamic)) ORDER BY k, t DESC;
SELECT 'Tuple held by Dynamic';
SELECT k, hex(t.1) FROM t_rio_tuple WHERE t = CAST(tuple(toFixedString('V0', 3)) AS Dynamic) ORDER BY k, t DESC;
-- One value matches, so `t` stays fixed.
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT k, t FROM t_rio_tuple WHERE t = tuple(CAST('V0' AS Dynamic)) ORDER BY k, t DESC) WHERE explain LIKE '%Prefix sort description%';
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
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT k, s FROM t_rio_fixed WHERE s = CAST(toFixedString('V0', 3) AS Dynamic) ORDER BY k, s DESC) WHERE explain LIKE '%Prefix sort description%';
DROP TABLE t_rio_fixed;

-- A `Dynamic` value equal to the constant can have several types, which sort differently: `1` as `Float64`, `Int8` and `UInt8`.
SELECT 'Dynamic column vs number';
DROP TABLE IF EXISTS t_rio_dynamic;
CREATE TABLE t_rio_dynamic (k UInt8, x UInt8, d Dynamic, j JSON) ENGINE = MergeTree ORDER BY (k, x);
INSERT INTO t_rio_dynamic SELECT 1, 1, 1::Float64, '{"a" : 1.0}';
INSERT INTO t_rio_dynamic SELECT 1, 2, 1::Int8, '{"a" : 1}';
INSERT INTO t_rio_dynamic SELECT 1, 3, 1::UInt8, '{"a" : 1}';
OPTIMIZE TABLE t_rio_dynamic FINAL;
SELECT k, dynamicType(d) FROM t_rio_dynamic WHERE d = 1 ORDER BY k, d DESC SETTINGS allow_suspicious_types_in_order_by = 1;
SELECT k, dynamicType(d) FROM t_rio_dynamic WHERE d = toInt8(1) ORDER BY k, d DESC SETTINGS allow_suspicious_types_in_order_by = 1;
SELECT 'JSON path vs number';
SELECT k, dynamicType(j.a) FROM t_rio_dynamic WHERE j.a = 1 ORDER BY k, j.a DESC, x SETTINGS allow_suspicious_types_in_order_by = 1;
DROP TABLE t_rio_dynamic;

DROP TABLE IF EXISTS t_rio_wrapped;
CREATE TABLE t_rio_wrapped (k UInt8, s String, x UInt8, j JSON) ENGINE = MergeTree ORDER BY (k, s, x);
INSERT INTO t_rio_wrapped VALUES (1, 'V0', 1, '{"a" : "V0"}'), (1, 'V0', 2, '{"a" : "V0"}'), (1, 'V0\0', 1, '{"a" : "V0\\u0000"}'), (1, 'V0\0', 2, '{"a" : "V0\\u0000"}');

SELECT 'String key, Variant(FixedString) constant';
SELECT k, hex(s), x FROM t_rio_wrapped WHERE s = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)) ORDER BY k, s DESC, x;
SELECT 'String key, Dynamic constant';
SELECT k, hex(s), x FROM t_rio_wrapped WHERE s = CAST(toFixedString('V0', 3) AS Dynamic) ORDER BY k, s DESC, x;
SELECT 'JSON path, FixedString constant';
SELECT k, hex(toString(j.a)) FROM t_rio_wrapped WHERE j.a = toFixedString('V0', 3) ORDER BY k, toString(j.a) DESC;
SELECT 'GROUP BY in order, Dynamic constant';
SELECT k, x, count() FROM t_rio_wrapped WHERE s = CAST(toFixedString('V0', 3) AS Dynamic) GROUP BY k, x ORDER BY k, x SETTINGS optimize_aggregation_in_order = 1;

DROP TABLE t_rio_wrapped;

-- `equals` compares a `Decimal` or `DateTime64` with a float in `Float64`, so `d = 1.0` matches both `1` and `1.000000000000000001`.
-- The in-order optimizations must not treat `d` as fixed and skip ordering or grouping by the next key column.

SET optimize_limit_by_in_order = 1, use_primary_key = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_rio_decimal_float;
CREATE TABLE t_rio_decimal_float (d Decimal(38, 18), ts UInt32) ENGINE = MergeTree ORDER BY (d, ts);
INSERT INTO t_rio_decimal_float VALUES ('1', 20), ('1', 30), ('1.000000000000000001', 10), ('1.000000000000000001', 20), ('2', 5);

SELECT 'Decimal(38, 18) key, Float64 constant';
SELECT ts FROM t_rio_decimal_float WHERE d = 1.0 ORDER BY ts;
SELECT ts FROM t_rio_decimal_float WHERE d = 1.0 ORDER BY ts LIMIT 1;
SELECT count() FROM (SELECT ts FROM t_rio_decimal_float WHERE d = 1.0 LIMIT 1 BY ts);
SELECT 'GROUP BY in order';
SELECT ts, count() FROM t_rio_decimal_float WHERE d = toLowCardinality(toNullable(1.0)) GROUP BY ts ORDER BY ts SETTINGS optimize_aggregation_in_order = 1;
SELECT 'Float32 held by Dynamic';
SELECT ts FROM t_rio_decimal_float WHERE d = CAST(toFloat32(1) AS Dynamic) ORDER BY ts;
-- An integer constant is compared exactly, so `d` stays fixed.
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT ts FROM t_rio_decimal_float WHERE d = 1 ORDER BY ts) WHERE explain LIKE '%Prefix sort description%';
DROP TABLE t_rio_decimal_float;

-- `CAST` does not check the precision of a `Decimal`, so `Decimal(10, 0)` can hold `2^53` and `2^53 + 1`, which are equal in `Float64`.
SELECT 'Decimal(10, 0)';
DROP TABLE IF EXISTS t_rio_decimal64;
CREATE TABLE t_rio_decimal64 (d Decimal(10, 0), ts UInt32) ENGINE = MergeTree ORDER BY (d, ts);
INSERT INTO t_rio_decimal64 SELECT CAST(d AS Decimal(10, 0)), ts
FROM values('d UInt64, ts UInt32', (9007199254740992, 20), (9007199254740992, 30), (9007199254740993, 10), (9007199254740993, 20));
SELECT ts FROM t_rio_decimal64 WHERE d = 9007199254740992. ORDER BY ts;
DROP TABLE t_rio_decimal64;

-- `Float64` keeps distinct `Decimal32` values distinct, so `d = 1.5` matches one value and `d` stays fixed.
DROP TABLE IF EXISTS t_rio_decimal32;
CREATE TABLE t_rio_decimal32 (d Decimal(9, 2), ts UInt32) ENGINE = MergeTree ORDER BY (d, ts);
INSERT INTO t_rio_decimal32 VALUES ('1.5', 20), ('1.5', 30), ('1.51', 10);
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT ts FROM t_rio_decimal32 WHERE d = 1.5 ORDER BY ts) WHERE explain LIKE '%Prefix sort description%';
DROP TABLE t_rio_decimal32;

SELECT 'DateTime64(9)';
DROP TABLE IF EXISTS t_rio_datetime64;
CREATE TABLE t_rio_datetime64 (t DateTime64(9, 'UTC'), ts UInt32) ENGINE = MergeTree ORDER BY (t, ts);
INSERT INTO t_rio_datetime64 VALUES ('2020-01-01 00:00:00', 20), ('2020-01-01 00:00:00', 30), ('2020-01-01 00:00:00.000000001', 10), ('2020-01-01 00:00:00.000000001', 20);
SELECT ts FROM t_rio_datetime64 WHERE t = 1577836800. ORDER BY ts;
DROP TABLE t_rio_datetime64;

SELECT 'Tuple(Decimal(76, 18))';
DROP TABLE IF EXISTS t_rio_decimal_tuple;
CREATE TABLE t_rio_decimal_tuple (t Tuple(Decimal(76, 18)), ts UInt32) ENGINE = MergeTree ORDER BY (t, ts);
INSERT INTO t_rio_decimal_tuple VALUES (tuple('1'), 20), (tuple('1'), 30), (tuple('1.000000000000000001'), 10), (tuple('1.000000000000000001'), 20);
SELECT ts FROM t_rio_decimal_tuple WHERE t = tuple(1.0) ORDER BY ts;
DROP TABLE t_rio_decimal_tuple;

-- `Array` values without a common type are compared element by element.
SELECT 'Array(Decimal(38, 18))';
DROP TABLE IF EXISTS t_rio_decimal_array;
CREATE TABLE t_rio_decimal_array (a Array(Decimal(38, 18)), ts UInt32) ENGINE = MergeTree ORDER BY (a, ts);
INSERT INTO t_rio_decimal_array VALUES (['1'], 20), (['1'], 30), (['1.000000000000000001'], 10), (['1.000000000000000001'], 20);
SELECT ts FROM t_rio_decimal_array WHERE a = [1.0] ORDER BY ts;
-- `[1]` has a common type with `a` and is cast to the type of `a`, so it matches one value and `a` stays fixed.
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT ts FROM t_rio_decimal_array WHERE a = [1] ORDER BY ts) WHERE explain LIKE '%Prefix sort description%';
DROP TABLE t_rio_decimal_array;

-- `UInt64` and `Int64` have no common type either, and a `String` element is compared zero-padded with a `FixedString` one.
SELECT 'Array(Tuple(String, UInt64))';
DROP TABLE IF EXISTS t_rio_array_string;
CREATE TABLE t_rio_array_string (a Array(Tuple(String, UInt64)), ts UInt32) ENGINE = MergeTree ORDER BY (a, ts);
INSERT INTO t_rio_array_string VALUES ([('V0', 1)], 20), ([('V0', 1)], 30), ([('V0\0', 1)], 10), ([('V0\0', 1)], 20);
SELECT ts FROM t_rio_array_string WHERE a = [tuple(toFixedString('V0', 3), toInt64(1))] ORDER BY ts;
DROP TABLE t_rio_array_string;

-- `equals` looks for the common type `Array(DateTime64(0))` without `LowCardinality`, which cannot hold a `DateTime64`.
SELECT 'Array(LowCardinality(DateTime)) vs Array(LowCardinality(Date32))';
DROP TABLE IF EXISTS t_rio_datetime;
CREATE TABLE t_rio_datetime (d DateTime, ts UInt32) ENGINE = MergeTree ORDER BY (d, ts);
INSERT INTO t_rio_datetime VALUES ('2020-01-01 00:00:00', 20), ('2020-01-01 00:00:00', 10), ('2021-01-01 00:00:00', 5);
SELECT ts FROM t_rio_datetime WHERE [toLowCardinality(d)] = [toLowCardinality(toDate32('2020-01-01'))] ORDER BY ts;
DROP TABLE t_rio_datetime;

-- `equals` parses a string literal into the type of a `Tuple` column, so `t = '(1)'` on a `Tuple(Dynamic)` matches `1` stored as
-- `Float64` and as `Int64`. Read-in-order must not treat `t` as fixed and skip sorting by `dynamicType(t.1)`.

DROP TABLE IF EXISTS t_rio_tuple_dynamic;
CREATE TABLE t_rio_tuple_dynamic (k UInt8, x UInt8, t Tuple(Dynamic), tt Tuple(Tuple(Dynamic)), tv Tuple(Variant(Float64, Int64)), j JSON)
ENGINE = MergeTree ORDER BY (k, x);
INSERT INTO t_rio_tuple_dynamic SELECT 1, 1, tuple(CAST(toFloat64(1) AS Dynamic)), tuple(tuple(CAST(toFloat64(1) AS Dynamic))),
    tuple(CAST(toFloat64(1) AS Variant(Float64, Int64))), '{"a" : 1.0}';
INSERT INTO t_rio_tuple_dynamic SELECT 1, 2, tuple(CAST(toInt64(1) AS Dynamic)), tuple(tuple(CAST(toInt64(1) AS Dynamic))),
    tuple(CAST(toInt64(1) AS Variant(Float64, Int64))), '{"a" : 1}';
-- The order is wrong only within one part, a merge of the parts sorts by every column of the prefix.
OPTIMIZE TABLE t_rio_tuple_dynamic FINAL;

SELECT 'Tuple(Dynamic)';
SELECT k, x, dynamicType(t.1) AS ty FROM t_rio_tuple_dynamic WHERE t = '(1)' ORDER BY k, ty DESC;
SELECT 'Tuple(Dynamic) vs FixedString';
SELECT k, x, dynamicType(t.1) AS ty FROM t_rio_tuple_dynamic WHERE t = toFixedString('(1)', 3) ORDER BY k, ty DESC;
SELECT 'Tuple(Tuple(Dynamic))';
SELECT k, x, dynamicType(tupleElement(tt.1, 1)) AS ty FROM t_rio_tuple_dynamic WHERE tt = '((1))' ORDER BY k, ty DESC;
SELECT 'Tuple(Variant(Float64, Int64))';
SELECT k, x, variantType(tv.1) AS ty FROM t_rio_tuple_dynamic WHERE tv = '(1)' ORDER BY k, ty DESC;
SELECT 'JSON path in a Tuple';
SELECT k, x, dynamicType(j.a) AS ty FROM t_rio_tuple_dynamic WHERE tuple(j.a) = '(1)' ORDER BY k, ty DESC;
DROP TABLE t_rio_tuple_dynamic;

-- The elements of a `Tuple(String)` are compared exactly, so `s` stays fixed and read-in-order keeps `x` in the prefix.
DROP TABLE IF EXISTS t_rio_tuple_string;
CREATE TABLE t_rio_tuple_string (k UInt8, s Tuple(String), x UInt8) ENGINE = MergeTree ORDER BY (k, s, x);
INSERT INTO t_rio_tuple_string VALUES (1, ('a'), 1), (1, ('a'), 2);
SELECT extract(explain, 'Prefix sort description: .*') FROM (EXPLAIN actions = 1 SELECT x FROM t_rio_tuple_string WHERE s = '(\'a\')' ORDER BY k, x) WHERE explain LIKE '%Prefix sort description%';
DROP TABLE t_rio_tuple_string;
