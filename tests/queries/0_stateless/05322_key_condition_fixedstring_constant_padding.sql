-- Casting a `FixedString` constant or set element to `String` drops its trailing zero bytes, so it must not be
-- pushed through a key expression this way: `s <= toFixedString('abc', 4)` would become `key <= 'abc'`. The same
-- holds for a `FixedString` nested in another type or held by a `Variant` or `Dynamic` value.

SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_monotonic;
DROP TABLE IF EXISTS t_deterministic;
DROP TABLE IF EXISTS t_bare;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_tuple_hashed;
DROP TABLE IF EXISTS t_uint8;
DROP TABLE IF EXISTS t_uint8_monotonic;
DROP TABLE IF EXISTS t_named_tuple;
DROP TABLE IF EXISTS t_tuple_parts;
DROP TABLE IF EXISTS t_partitioned;
DROP TABLE IF EXISTS t_named_partitioned;
DROP TABLE IF EXISTS t_named_hashed;
DROP TABLE IF EXISTS t_named_monotonic;
DROP TABLE IF EXISTS t_named_dynamic;
DROP TABLE IF EXISTS t_dynamic_key;
DROP TABLE IF EXISTS t_map;

CREATE TABLE t_monotonic (s Nullable(String))
ENGINE = MergeTree ORDER BY assumeNotNull(s) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_monotonic VALUES ('abc'), ('abc\0'), ('abc\0'), ('b');

CREATE TABLE t_deterministic (s String)
ENGINE = MergeTree ORDER BY cityHash64(s) SETTINGS index_granularity = 1;
INSERT INTO t_deterministic VALUES ('abc'), ('abc\0'), ('abc\0'), ('b');

CREATE TABLE t_bare (s String)
ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO t_bare VALUES ('abc'), ('abc\0'), ('abc\0'), ('b');

CREATE TABLE t_array (s Array(String))
ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO t_array VALUES (['abc']), (['abc\0']), (['abc\0']), (['b']);

CREATE TABLE t_tuple (k Tuple(FixedString(4), String, UInt64))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_tuple VALUES (('abc', 'abc', 1)), (('abc', 'abc\0', 1)), (('abc', 'abc\0', 1)), (('b', 'b', 2));

CREATE TABLE t_tuple_hashed (x Tuple(String, UInt8))
ENGINE = MergeTree ORDER BY cityHash64(x) SETTINGS index_granularity = 1;
INSERT INTO t_tuple_hashed VALUES (('abc', 1)), (('abc\0', 1)), (('abc\0', 1)), (('b', 1));

CREATE TABLE t_uint8 (x UInt8)
ENGINE = MergeTree ORDER BY cityHash64(x) SETTINGS index_granularity = 1;
INSERT INTO t_uint8 VALUES (0), (1), (2), (3), (200);

CREATE TABLE t_uint8_monotonic (x Nullable(UInt8))
ENGINE = MergeTree ORDER BY assumeNotNull(x) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_uint8_monotonic VALUES (0), (1), (2), (3), (200);

-- One row per part, so that a wrong range prunes a whole part.
CREATE TABLE t_named_tuple (k Tuple(a FixedString(4), b String))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_named_tuple;
INSERT INTO t_named_tuple VALUES (('abc', 'abc'));
INSERT INTO t_named_tuple VALUES (('abc', 'abc\0'));
INSERT INTO t_named_tuple VALUES (('b', 'b'));

CREATE TABLE t_tuple_parts (k Tuple(FixedString(4), String, UInt64))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_tuple_parts;
INSERT INTO t_tuple_parts VALUES (('abc', 'abc', 1));
INSERT INTO t_tuple_parts VALUES (('abc', 'abc\0', 1));
INSERT INTO t_tuple_parts VALUES (('b', 'b', 2));

CREATE TABLE t_partitioned (k Tuple(String, UInt8))
ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_partitioned VALUES (('abc', 1)), (('abc\0', 1)), (('abc\0\0', 1)), (('b', 1));

CREATE TABLE t_named_partitioned (k Tuple(a FixedString(4), b String))
ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_named_partitioned VALUES (('abc', 'def')), (('abc', 'def\0')), (('abc', 'def\0\0')), (('def', 'abc'));

CREATE TABLE t_named_hashed (k Tuple(a FixedString(4), b String))
ENGINE = MergeTree ORDER BY cityHash64(k) SETTINGS index_granularity = 1;
INSERT INTO t_named_hashed VALUES (('abc', 'abc')), (('abc', 'abc\0')), (('abc', 'abc\0\0')), (('b', 'b'));

CREATE TABLE t_named_monotonic (k Nullable(Tuple(a String, b String)))
ENGINE = MergeTree ORDER BY assumeNotNull(k) SETTINGS index_granularity = 1, allow_nullable_key = 1;
SYSTEM STOP MERGES t_named_monotonic;
INSERT INTO t_named_monotonic VALUES (('x', 'y'));
INSERT INTO t_named_monotonic VALUES (('y', 'x'));
INSERT INTO t_named_monotonic VALUES (('z', 'z'));

CREATE TABLE t_named_dynamic (k Tuple(a String, b String))
ENGINE = MergeTree ORDER BY cityHash64(k) SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_named_dynamic;
INSERT INTO t_named_dynamic VALUES (('x', 'y'));
INSERT INTO t_named_dynamic VALUES (('y', 'x'));
INSERT INTO t_named_dynamic VALUES (('z', 'z'));

CREATE TABLE t_dynamic_key (k Dynamic, INDEX i CAST(k AS String) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY CAST(k AS String) SETTINGS index_granularity = 1;
INSERT INTO t_dynamic_key SELECT CAST('abc\0' AS Dynamic);
INSERT INTO t_dynamic_key SELECT CAST('x' AS Dynamic);

CREATE TABLE t_map (m Map(String, String))
ENGINE = MergeTree ORDER BY cityHash64(m) SETTINGS index_granularity = 1;
INSERT INTO t_map VALUES (map('a', 'abc')), (map('a', 'abc\0')), (map('a', 'b'));

-- { echo }

SELECT count() FROM t_monotonic WHERE s <= toFixedString('abc', 4);
SELECT count() FROM t_deterministic WHERE s = toFixedString('abc', 4);
SELECT count() FROM t_deterministic WHERE s IN (SELECT toFixedString('abc', 4));
SELECT count() FROM t_deterministic WHERE s = CAST(toFixedString('abc', 4) AS Variant(FixedString(4), UInt64));
SELECT count() FROM t_deterministic WHERE s = CAST(toFixedString('abc', 4) AS Dynamic);
SELECT count() FROM t_deterministic WHERE s IN (SELECT CAST(toFixedString('abc', 4) AS Variant(String, FixedString(4))));
SELECT count() FROM t_bare WHERE s IN (SELECT toFixedString('abc', 4));
SELECT count() FROM t_bare WHERE s IN (SELECT toNullable(toFixedString('abc', 4)));
SELECT count() FROM t_bare WHERE s IN (SELECT CAST(toFixedString('abc', 4) AS Dynamic));
SELECT count() FROM t_deterministic WHERE s = CAST(toFixedString('abc', 4) AS Dynamic(max_types = 0));
SELECT count() FROM t_array WHERE s IN (SELECT [toFixedString('abc', 4)]);
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), toFixedString('abc', 4), toUInt8(1)));
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), CAST(toFixedString('abc', 4) AS Variant(String, FixedString(4))), toUInt8(1)));
SELECT count() FROM t_tuple_hashed WHERE x = (CAST(toFixedString('abc', 4) AS Dynamic), toUInt8(1));
SELECT count() FROM t_tuple_hashed WHERE x = CAST((toFixedString('abc', 4), toUInt8(1)) AS Dynamic);
SELECT count() FROM t_tuple_hashed WHERE x = (CAST(toFixedString('abc', 4) AS Dynamic(max_types = 0)), toUInt8(1));
SELECT count() FROM t_tuple_parts WHERE k = tuple(toFixedString('abc', 4), toFixedString('abc', 4), 1);
SELECT count() FROM t_tuple_parts WHERE k = tuple(toFixedString('abc', 4), CAST(toFixedString('abc', 4) AS Dynamic), 1);
SELECT count() FROM t_named_tuple WHERE k IN (SELECT CAST(tuple(toFixedString('abc', 4), toFixedString('abc', 4)) AS Tuple(b FixedString(4), a FixedString(4))));
SELECT count() FROM t_partitioned WHERE k = (CAST(toFixedString('abc', 4) AS Dynamic), toUInt8(1)) SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM t_partitioned WHERE k = CAST((toFixedString('abc', 4), toUInt8(1)) AS Dynamic) SETTINGS optimize_trivial_count_query = 0;
-- A comparison pairs the elements of named tuples by position, not by name as `CAST` does.
SELECT count() FROM t_named_partitioned WHERE k = CAST(('abc', CAST(toFixedString('def', 4) AS Dynamic)) AS Tuple(b String, a Dynamic)) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
-- The constant is not converted to the key type by `CAST`, which would reorder the elements by name.
SELECT count() FROM t_named_hashed WHERE k = CAST(('abc', toFixedString('abc', 4)) AS Tuple(b String, a FixedString(4)));
SELECT count() FROM t_named_monotonic WHERE k = CAST(('x', 'y') AS Tuple(b String, a String));
SELECT count() FROM t_named_dynamic WHERE k = CAST(CAST(('x', 'y') AS Tuple(b String, a String)) AS Dynamic);
-- A key expression that reads a `Dynamic` drops the padding of the `FixedString` the constant holds.
SELECT count() FROM t_dynamic_key WHERE k = CAST(toFixedString('abc', 4) AS Dynamic) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_dynamic_key WHERE k = CAST(toFixedString('abc', 4) AS Dynamic) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, use_primary_key = 0;

-- A `Variant` or `Dynamic` value that holds a `String` keeps index analysis, even when its type allows a `FixedString`.
SELECT count() FROM t_deterministic WHERE s = CAST('abc' AS Variant(String, FixedString(2))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_deterministic WHERE s = CAST('abc' AS Dynamic) SETTINGS force_primary_key = 1;
SELECT count() FROM t_monotonic WHERE s <= CAST('abc' AS Variant(String, FixedString(2))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), CAST('abc' AS Variant(String, FixedString(2))), toUInt8(1))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), CAST('abc' AS Dynamic), toUInt8(1))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_tuple_hashed WHERE x = (CAST('abc' AS Dynamic), toUInt8(1)) SETTINGS force_primary_key = 1;
-- A `Dynamic` without its own types stores the value in its shared variant.
SELECT count() FROM t_tuple_hashed WHERE x = (CAST('abc' AS Dynamic(max_types = 0)), toUInt8(1)) SETTINGS force_primary_key = 1;
SELECT count() FROM t_partitioned WHERE k = (CAST('abc' AS Dynamic), toUInt8(1)) SETTINGS optimize_trivial_count_query = 0, force_index_by_date = 1;

-- A `FixedString` that stays a `FixedString` keeps index analysis.
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), 'abc', toUInt8(1))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_tuple WHERE k = tuple(toFixedString('abc', 4), 'abc', 1) SETTINGS force_primary_key = 1;
SELECT count() FROM t_named_monotonic WHERE k = CAST(('x', 'y') AS Tuple(a String, b String)) SETTINGS force_primary_key = 1;
-- A `Variant` or `Dynamic` constant is judged by the named tuple it actually holds.
SELECT count() FROM t_named_dynamic WHERE k = CAST(CAST(('x', 'y') AS Tuple(a String, b String)) AS Dynamic) SETTINGS force_primary_key = 1;
SELECT count() FROM t_named_dynamic WHERE k = CAST(CAST(('x', 'y') AS Tuple(a String, b String)) AS Variant(Tuple(a String, b String), Tuple(b String, a String))) SETTINGS force_primary_key = 1;
-- Named tuple elements are paired by name, as `CAST` does.
SELECT count() FROM t_named_tuple WHERE k IN (SELECT CAST(tuple('abc', toFixedString('abc', 4)) AS Tuple(b String, a FixedString(4)))) SETTINGS force_primary_key = 1;
-- A `Map` without a `FixedString` keeps index analysis.
SELECT count() FROM t_map WHERE m = map('a', 'abc') SETTINGS force_primary_key = 1;

-- A `Variant` or `Dynamic` constant keeps the conversions of its declared type.
SELECT count() FROM t_uint8 WHERE x = CAST(toIPv4(2) AS Dynamic);
SELECT count() FROM t_uint8 WHERE x = CAST(toIPv4(2) AS Variant(IPv4, UInt8));
SELECT count() FROM t_uint8_monotonic WHERE x < CAST(toIPv4(2) AS Dynamic);

DROP TABLE t_monotonic;
DROP TABLE t_deterministic;
DROP TABLE t_bare;
DROP TABLE t_array;
DROP TABLE t_tuple;
DROP TABLE t_tuple_hashed;
DROP TABLE t_uint8;
DROP TABLE t_uint8_monotonic;
DROP TABLE t_named_tuple;
DROP TABLE t_tuple_parts;
DROP TABLE t_partitioned;
DROP TABLE t_named_partitioned;
DROP TABLE t_named_hashed;
DROP TABLE t_named_monotonic;
DROP TABLE t_named_dynamic;
DROP TABLE t_dynamic_key;
DROP TABLE t_map;
