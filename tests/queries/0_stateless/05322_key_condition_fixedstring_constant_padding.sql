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
SELECT count() FROM t_array WHERE s IN (SELECT [toFixedString('abc', 4)]);
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), toFixedString('abc', 4), toUInt8(1)));
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), CAST(toFixedString('abc', 4) AS Variant(String, FixedString(4))), toUInt8(1)));
SELECT count() FROM t_tuple_hashed WHERE x = (CAST(toFixedString('abc', 4) AS Dynamic), toUInt8(1));
SELECT count() FROM t_tuple_hashed WHERE x = CAST((toFixedString('abc', 4), toUInt8(1)) AS Dynamic);
SELECT count() FROM t_tuple_parts WHERE k = tuple(toFixedString('abc', 4), toFixedString('abc', 4), 1);
SELECT count() FROM t_tuple_parts WHERE k = tuple(toFixedString('abc', 4), CAST(toFixedString('abc', 4) AS Dynamic), 1);
SELECT count() FROM t_named_tuple WHERE k IN (SELECT CAST(tuple(toFixedString('abc', 4), toFixedString('abc', 4)) AS Tuple(b FixedString(4), a FixedString(4))));

-- A `Variant` or `Dynamic` value that holds a `String` keeps index analysis, even when its type allows a `FixedString`.
SELECT count() FROM t_deterministic WHERE s = CAST('abc' AS Variant(String, FixedString(2))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_deterministic WHERE s = CAST('abc' AS Dynamic) SETTINGS force_primary_key = 1;
SELECT count() FROM t_monotonic WHERE s <= CAST('abc' AS Variant(String, FixedString(2))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), CAST('abc' AS Variant(String, FixedString(2))), toUInt8(1))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), CAST('abc' AS Dynamic), toUInt8(1))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_tuple_hashed WHERE x = (CAST('abc' AS Dynamic), toUInt8(1)) SETTINGS force_primary_key = 1;

-- A `FixedString` that stays a `FixedString` keeps index analysis.
SELECT count() FROM t_tuple WHERE k IN (SELECT tuple(toFixedString('abc', 4), 'abc', toUInt8(1))) SETTINGS force_primary_key = 1;
SELECT count() FROM t_tuple WHERE k = tuple(toFixedString('abc', 4), 'abc', 1) SETTINGS force_primary_key = 1;
-- Named tuple elements are paired by name, as `CAST` does.
SELECT count() FROM t_named_tuple WHERE k IN (SELECT CAST(tuple('abc', toFixedString('abc', 4)) AS Tuple(b String, a FixedString(4)))) SETTINGS force_primary_key = 1;

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
