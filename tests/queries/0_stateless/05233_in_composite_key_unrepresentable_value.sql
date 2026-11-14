-- At transform_null_in = 0 a left-hand key value the set's key type cannot represent makes its row not
-- in the set (and in the set for NOT IN), instead of aborting the query. A single-column key has always
-- behaved that way; a Tuple key does too, for the element shapes whose failed conversion the
-- accurate-or-null cast can report. The controls pin the shapes it cannot report, which keep aborting.

DROP TABLE IF EXISTS t_05233_left;
DROP TABLE IF EXISTS t_05233_right;
DROP TABLE IF EXISTS t_05233_left_ok;
DROP TABLE IF EXISTS t_05233_left_nullable;
DROP TABLE IF EXISTS t_05233_left_int16;
DROP TABLE IF EXISTS t_05233_left_array;
DROP TABLE IF EXISTS t_05233_left_300;
DROP TABLE IF EXISTS t_05233_mt;
DROP TABLE IF EXISTS t_05233_left_variant;
DROP TABLE IF EXISTS t_05233_right_variant;

CREATE TABLE t_05233_left (s String, g UInt8) ENGINE = Memory;
INSERT INTO t_05233_left VALUES ('abc', 1), ('42', 1);

CREATE TABLE t_05233_right (v Nullable(Int32), g UInt8) ENGINE = Memory;
INSERT INTO t_05233_right VALUES (42, 1);

CREATE TABLE t_05233_left_ok (s String, g UInt8) ENGINE = Memory;
INSERT INTO t_05233_left_ok VALUES ('42', 1), ('7', 1);

CREATE TABLE t_05233_left_nullable (s Nullable(String), g UInt8) ENGINE = Memory;
INSERT INTO t_05233_left_nullable VALUES (NULL, 1), ('42', 1);

CREATE TABLE t_05233_left_int16 (n Int16, g UInt8) ENGINE = Memory;
INSERT INTO t_05233_left_int16 VALUES (300, 1), (42, 1);

CREATE TABLE t_05233_left_array (a Array(Int32), g UInt8) ENGINE = Memory;
INSERT INTO t_05233_left_array VALUES ([1], 1), ([2], 1);

CREATE TABLE t_05233_left_300 (s String, n8 Int8, fs FixedString(3), d Date, g UInt8) ENGINE = Memory;
INSERT INTO t_05233_left_300 VALUES ('300', 7, '300', '2100-01-01', 1);

CREATE TABLE t_05233_mt (s String, g UInt8) ENGINE = MergeTree ORDER BY (s, g);
INSERT INTO t_05233_mt VALUES ('abc', 1), ('42', 1);

SELECT 'tuple key, unparsable text' AS arm, count()
FROM t_05233_left WHERE (s, g) IN (SELECT (v, g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

SELECT 'tuple key, NOT IN' AS arm, count()
FROM t_05233_left WHERE (s, g) NOT IN (SELECT (v, g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

SELECT 'tuple key, value out of the element range' AS arm, count()
FROM t_05233_left_int16 WHERE (n, g) IN (SELECT (CAST(v, 'Nullable(Int8)'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

-- The same fixture with the wrap-around artifact 44 in the set instead of 42: a value the element type
-- cannot represent is a non-member, not a match on the value a truncating conversion would produce.
SELECT 'tuple key, out of range does not match the wrap artifact' AS arm, count()
FROM t_05233_left_int16 WHERE (n, g) IN (SELECT (CAST(44, 'Nullable(Int8)'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

-- An unrepresentable value must not be coerced to a NULL that then matches a genuine NULL in the set.
SELECT 'tuple key, set holds only NULL' AS arm, count()
FROM t_05233_left WHERE (s, g) IN (SELECT (CAST(NULL, 'Nullable(Int32)'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

-- A NULL the source carried is a key value and is compared as any other, unlike a conversion failure.
SELECT 'tuple key, source NULL matches set NULL' AS arm, count()
FROM t_05233_left_nullable WHERE (s, g) IN (SELECT (CAST(NULL, 'Nullable(Int32)'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

SELECT 'tuple key, every value representable' AS arm, count()
FROM t_05233_left_ok WHERE (s, g) IN (SELECT (v, g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

SELECT 'tuple key, literal set' AS arm, count()
FROM t_05233_left WHERE (s, g) IN ((42, 1))
SETTINGS transform_null_in = 0;

SELECT 'tuple key over a primary key' AS arm, count()
FROM t_05233_mt WHERE (s, g) IN (SELECT (v, g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

SELECT 'single column key' AS arm, count()
FROM t_05233_left WHERE s IN (SELECT v FROM t_05233_right)
SETTINGS transform_null_in = 0;

-- A Variant element holds its NULLs outside a Nullable, and such a NULL is a key value like any other
-- rather than a failed conversion of the tuple: the row is a member, and is not a member of NOT IN.
CREATE TABLE t_05233_left_variant (v Variant(Int32, String), s String) ENGINE = Memory;
INSERT INTO t_05233_left_variant VALUES (NULL, '42');

CREATE TABLE t_05233_right_variant (v Variant(Int32, String), n Nullable(Int32)) ENGINE = Memory;
INSERT INTO t_05233_right_variant VALUES (NULL, 42);

SELECT 'variant element NULL is a key value' AS arm, count()
FROM t_05233_left_variant WHERE (v, s) IN (SELECT (v, n) FROM t_05233_right_variant)
SETTINGS transform_null_in = 0;

SELECT 'variant element NULL, NOT IN' AS arm, count()
FROM t_05233_left_variant WHERE (v, s) NOT IN (SELECT (v, n) FROM t_05233_right_variant)
SETTINGS transform_null_in = 0;

-- LowCardinality is an encoding the cast unwraps before converting and re-applies afterwards, so a
-- LowCardinality(Nullable(<numeric>)) element reports a failed conversion as a plain Nullable one does.
-- Such an element type is only creatable with allow_suspicious_low_cardinality_types.
SELECT 'tuple key, LowCardinality(Nullable) set element' AS arm, count()
FROM t_05233_left_int16 WHERE (n, g) IN (SELECT (CAST(v, 'LowCardinality(Nullable(Int8))'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0, allow_suspicious_low_cardinality_types = 1;

SELECT 'tuple key, LowCardinality(Nullable) does not match the wrap artifact' AS arm, count()
FROM t_05233_left_int16 WHERE (n, g) IN (SELECT (CAST(44, 'LowCardinality(Nullable(Int8))'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0, allow_suspicious_low_cardinality_types = 1;

-- A failed conversion of such an element must set the tuple-level failure mask rather than become an
-- inner NULL: as an inner NULL it would match a genuine NULL key, which answers 1 here.
SELECT 'tuple key, LowCardinality(Nullable) set holds only NULL' AS arm, count()
FROM t_05233_left_int16 WHERE (n, g) IN (SELECT (CAST(NULL, 'LowCardinality(Nullable(Int8))'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0, allow_suspicious_low_cardinality_types = 1;

-- The other direction through the dictionary unwrap: a NULL the source carried is a key value and does
-- match. This answers 1 on the strict path too; it is live against the source-NULL subtraction.
SELECT 'tuple key, LowCardinality(Nullable) source NULL matches set NULL' AS arm, count()
FROM t_05233_left_nullable WHERE (s, g) IN (SELECT (CAST(NULL, 'LowCardinality(Nullable(Int8))'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0, allow_suspicious_low_cardinality_types = 1;

SELECT 'tuple key, LowCardinality source element' AS arm, count()
FROM t_05233_left WHERE (CAST(s, 'LowCardinality(String)'), g) IN (SELECT (v, g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

-- Controls. Each set key type below is one the cast cannot report a failed element for, so the query
-- keeps aborting; admitting any of them would answer a match on a value the left row does not hold.

-- Element target is not Nullable, so a failure has nowhere to be reported and the element would
-- silently become 44.
SELECT count() FROM t_05233_left_300
WHERE (s, g) IN (SELECT (CAST(44, 'Int8'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0; -- { serverError CANNOT_PARSE_TEXT }

-- A LowCardinality element whose dictionary type is not Nullable has nowhere to report a failure either.
SELECT count() FROM t_05233_left_300
WHERE (s, g) IN (SELECT (CAST(44, 'LowCardinality(Int8)'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0, allow_suspicious_low_cardinality_types = 1; -- { serverError CANNOT_PARSE_TEXT }

-- Element target is Nullable(<composite>): the cast drops the request for a composite wrapper, and the
-- element would silently become (44).
SELECT count() FROM t_05233_left_300
WHERE (tuple(s), g) IN (SELECT (CAST(tuple(toInt8(44)), 'Nullable(Tuple(Int8))'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0; -- { serverError CANNOT_PARSE_TEXT }

-- Both tuples are named and the names reorder the pairing, so the text is routed into the
-- non-Nullable element and would silently become 44.
SELECT count() FROM t_05233_left_300
WHERE CAST((n8, s), 'Tuple(a Int8, b String)') IN (SELECT CAST((toInt8(44), toInt8(7)), 'Tuple(b Int8, a Nullable(Int8))') FROM t_05233_right)
SETTINGS transform_null_in = 0; -- { serverError CANNOT_PARSE_TEXT }

-- FixedString source: the cast's null-returning text parser recognises a String source only, so the
-- element would silently become 44.
SELECT count() FROM t_05233_left_300
WHERE (fs, g) IN (SELECT (CAST(44, 'Nullable(Int8)'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0; -- { serverError CANNOT_PARSE_TEXT }

-- A Tuple element that cannot be inside Nullable keeps its own path; the cast would refuse the target.
SELECT 'tuple of array key' AS arm, count()
FROM t_05233_left_array WHERE (a, g) IN (SELECT (CAST([1], 'Array(Int32)'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

-- Date source: its conversion truncates to a day number instead of reporting, which matches 122 here.
-- That answer is the one this test found and is not what the exclusion is for; it is unchanged.
SELECT 'date source element' AS arm, count()
FROM t_05233_left_300 WHERE (d, g) IN (SELECT (CAST(122, 'Nullable(Int8)'), g) FROM t_05233_right)
SETTINGS transform_null_in = 0;

-- transform_null_in = 1 keeps the value-conversion semantics for every key shape.
SELECT count() FROM t_05233_left
WHERE (s, g) IN (SELECT (v, g) FROM t_05233_right)
SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }

DROP TABLE t_05233_left;
DROP TABLE t_05233_right;
DROP TABLE t_05233_left_ok;
DROP TABLE t_05233_left_nullable;
DROP TABLE t_05233_left_int16;
DROP TABLE t_05233_left_array;
DROP TABLE t_05233_left_300;
DROP TABLE t_05233_mt;
DROP TABLE t_05233_left_variant;
DROP TABLE t_05233_right_variant;
