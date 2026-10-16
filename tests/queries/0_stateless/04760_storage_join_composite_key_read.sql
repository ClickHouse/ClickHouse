-- Reading a Join table directly (SELECT ... FROM <join table>), for composite and wide keys.
-- Reproduces https://github.com/ClickHouse/ClickHouse/issues/74362 and
-- https://github.com/ClickHouse/ClickHouse/issues/77394, which failed with
-- "Unsupported JOIN keys of type keys256 in StorageJoin".
-- 04305_storage_join_composite_keys covers composite keys but only ever joins *against* the
-- table, so the scan path stayed unpinned.

DROP TABLE IF EXISTS tj;

SELECT 'three UInt64 keys (keys256)';
CREATE TABLE tj (key1 UInt64, key2 UInt64, key3 UInt64, w UInt64)
    ENGINE = Join(ALL, INNER, key1, key2, key3);
INSERT INTO tj SELECT number, number + 1, number + 2, number * 100 FROM numbers(5);
SELECT * FROM tj ORDER BY key1;
-- The dispatch runs before any column is touched, so a non-key column alone used to throw too.
SELECT w FROM tj ORDER BY w;
DROP TABLE tj;

SELECT 'two UInt64 keys (keys128)';
CREATE TABLE tj (a UInt64, b UInt64, w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj SELECT number, number + 500, number * 10 FROM numbers(3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

-- Unequal key widths make packFixedBatch order the components widest-first, so the byte order
-- is not the clause order here. A UInt16 decoded as a UInt8 cannot reach 4000.
SELECT 'UInt8 + UInt16 keys (keys32, packed widest-first)';
CREATE TABLE tj (a UInt8, b UInt16, w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj SELECT toUInt8(number + 7), toUInt16(number + 4000), number * 10 FROM numbers(3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

-- A key wider than 16 bytes cannot use the prepared-keys path, so this one is packed in clause
-- order despite the widths differing. Together with the arm above it pins both layouts.
SELECT 'UInt64 + UInt8 + UInt128 keys (keys256, packed in clause order)';
CREATE TABLE tj (a UInt64, b UInt8, c UInt128, w UInt64) ENGINE = Join(ALL, INNER, a, b, c);
INSERT INTO tj SELECT number + 900, toUInt8(number + 3), toUInt128(number + 70000), number * 10 FROM numbers(3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

SELECT 'two UInt32 keys (keys64)';
CREATE TABLE tj (a UInt32, b UInt32, w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj SELECT toUInt32(number), toUInt32(number + 500), number * 10 FROM numbers(3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

SELECT 'single wide numeric key';
CREATE TABLE tj (a UUID, w UInt64) ENGINE = Join(ALL, INNER, a);
INSERT INTO tj VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 7);
SELECT * FROM tj;
DROP TABLE tj;

CREATE TABLE tj (a Int256, w UInt64) ENGINE = Join(ALL, INNER, a);
INSERT INTO tj VALUES (-12345678901234567890, 7);
SELECT * FROM tj;
DROP TABLE tj;

SELECT 'FixedString keys';
CREATE TABLE tj (a FixedString(4), b UInt64, w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj VALUES ('abcd', 7, 10), ('efgh', 8, 20);
SELECT * FROM tj ORDER BY b;
DROP TABLE tj;

CREATE TABLE tj (a FixedString(2), b FixedString(2), w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj VALUES ('ab', 'cd', 10), ('ef', 'gh', 20);
SELECT * FROM tj ORDER BY w;
DROP TABLE tj;

-- Three of the four packed bytes are used, so the key is narrower than the map key.
CREATE TABLE tj (a FixedString(3), w UInt64) ENGINE = Join(ALL, INNER, a);
INSERT INTO tj VALUES ('abc', 10), ('def', 20);
SELECT * FROM tj ORDER BY w;
DROP TABLE tj;

SELECT 'four keys';
CREATE TABLE tj (a UInt64, b UInt64, c UInt64, d UInt64, w UInt64)
    ENGINE = Join(ALL, INNER, a, b, c, d);
INSERT INTO tj SELECT number, number + 10, number + 20, number + 30, number * 7 FROM numbers(3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

-- A key name repeated in the engine arguments occupies one packed slot per argument, while the
-- key block holds it once, so offsets taken from that block would misread b.
SELECT 'repeated engine key name';
CREATE TABLE tj (a UInt64, b UInt64, w UInt64) ENGINE = Join(ALL, INNER, a, a, b);
INSERT INTO tj SELECT number, number + 1000, number * 10 FROM numbers(3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

SELECT 'a subset of the key columns, reordered';
CREATE TABLE tj (a UInt8, b UInt16, c UInt32, w UInt64) ENGINE = Join(ALL, INNER, a, b, c);
INSERT INTO tj SELECT toUInt8(number + 7), toUInt16(number + 4000), toUInt32(number + 90000), number * 10 FROM numbers(3);
SELECT c, a FROM tj ORDER BY a;
SELECT b, w FROM tj ORDER BY b;
DROP TABLE tj;

SELECT 'ANY INNER (one row per key)';
CREATE TABLE tj (a UInt64, b UInt64, w UInt64) ENGINE = Join(ANY, INNER, a, b);
INSERT INTO tj SELECT number, number + 5, number * 10 FROM numbers(3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

SELECT 'ALL RIGHT (the key columns are also kept in the stored block)';
CREATE TABLE tj (a UInt64, b UInt64, w UInt64) ENGINE = Join(ALL, RIGHT, a, b);
INSERT INTO tj SELECT number, number + 5, number * 10 FROM numbers(3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

SELECT 'several rows per key';
CREATE TABLE tj (a UInt64, b UInt64, w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj VALUES (1, 2, 10), (1, 2, 20), (1, 2, 30), (5, 6, 50);
SELECT * FROM tj ORDER BY a, w;
DROP TABLE tj;

-- A NULL in any key component is not inserted into the map, so it is not read back either.
SELECT 'Nullable key component';
CREATE TABLE tj (a Nullable(UInt64), b UInt64, w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj VALUES (1, 2, 10), (NULL, 3, 20), (4, 5, 40);
SELECT * FROM tj ORDER BY b;
DROP TABLE tj;

-- Scanning must agree with probing, which was already correct.
SELECT 'the scanned rows match the joined rows';
CREATE TABLE tj (a UInt8, b UInt16, w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj SELECT toUInt8(number + 7), toUInt16(number + 4000), number * 10 FROM numbers(20);
CREATE TABLE src AS tj ENGINE = Memory;
INSERT INTO src SELECT * FROM tj;
SELECT (SELECT groupArray(x) FROM (SELECT cityHash64(*) AS x FROM tj ORDER BY x))
     = (SELECT groupArray(x) FROM (SELECT cityHash64(t.*) AS x FROM src AS s ALL INNER JOIN tj AS t USING (a, b) ORDER BY x));
DROP TABLE src;
DROP TABLE tj;

SELECT 'single-column keys are unchanged';
CREATE TABLE tj (a UInt64, w UInt64) ENGINE = Join(ALL, INNER, a);
INSERT INTO tj SELECT number, number * 10 FROM numbers(3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

CREATE TABLE tj (a String, w UInt64) ENGINE = Join(ALL, INNER, a);
INSERT INTO tj VALUES ('x', 1), ('yy', 2), ('', 3);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

CREATE TABLE tj (a LowCardinality(String), w UInt64) ENGINE = Join(ALL, INNER, a);
INSERT INTO tj VALUES ('x', 1), ('yy', 2);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

CREATE TABLE tj (a Nullable(UInt64), w UInt64) ENGINE = Join(ALL, INNER, a);
INSERT INTO tj VALUES (1, 10), (NULL, 20), (3, 30);
SELECT * FROM tj ORDER BY a;
DROP TABLE tj;

-- A hashed map stores hash128 of the key values, so the values cannot be recovered from it.
SELECT 'keys stored as a hash are still rejected';
CREATE TABLE tj (a UInt64, b String, w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj VALUES (1, 'x', 10);
SELECT * FROM tj; -- { serverError UNSUPPORTED_JOIN_KEYS }
DROP TABLE tj;

CREATE TABLE tj (a LowCardinality(String), b UInt64, w UInt64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO tj VALUES ('x', 1, 10);
SELECT * FROM tj; -- { serverError UNSUPPORTED_JOIN_KEYS }
DROP TABLE tj;
