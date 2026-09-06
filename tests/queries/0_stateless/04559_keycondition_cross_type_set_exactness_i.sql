-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

-- Part of the 04549/04552-04562 family: one set-index exactness suite split across files to fit
-- the flaky check's 180s per-test budget. Every part is self-contained.

SET explain_query_plan_default = 'legacy';
SET optimize_use_implicit_projections = 0;
-- A randomized `compatibility` below 25.12 reverts this setting to false, and the `Time64` cells then
-- fail to create their column. A session `SET` survives that: the compatibility pass skips settings
-- already changed manually.
SET enable_time_time64_type = 1;
-- The set elements below that spell `DateTime` without a zone take it from the session, which the test
-- runner randomizes; pin it so the no-zone/zone pair stays the discriminator by construction.
SET session_timezone = 'UTC';

-- A set-index atom may only be treated as an exact image of the predicate when the conversion
-- preserves equality in BOTH directions: index preparation casts the set values into the key type,
-- runtime membership casts the key into the set type. Every carrier below returned a WRONG result
-- (rows silently vanished) because a non-equality-preserving cast was treated as exact. Each carrier
-- asserts the MergeTree answer against an identical `ENGINE = Memory` oracle.

SELECT '--- integer composites: pruning is withdrawn for IN, results stay correct ---';
CREATE TABLE c_gcn_uint32 (kt Tuple(Nullable(UInt32), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_uint32 (kt Tuple(Nullable(UInt32), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_uint32 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_uint32 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid N UInt32/UInt8' AS c1,
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid N UInt32/UInt16' AS c1,
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid N UInt32/UInt32' AS c1,
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid N UInt32/UInt64' AS c1,
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid N UInt32/Int8' AS c1,
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid N UInt32/Int16' AS c1,
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid N UInt32/Int32' AS c1,
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid N UInt32/Int64' AS c1,
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gcn_uint32; DROP TABLE o_gcn_uint32;

DROP TABLE IF EXISTS c_gcn_uint64; DROP TABLE IF EXISTS o_gcn_uint64;
CREATE TABLE c_gcn_uint64 (kt Tuple(Nullable(UInt64), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_uint64 (kt Tuple(Nullable(UInt64), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_uint64 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_uint64 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid N UInt64/UInt8' AS c1,
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid N UInt64/UInt16' AS c1,
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid N UInt64/UInt32' AS c1,
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid N UInt64/UInt64' AS c1,
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid N UInt64/Int8' AS c1,
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid N UInt64/Int16' AS c1,
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid N UInt64/Int32' AS c1,
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid N UInt64/Int64' AS c1,
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gcn_uint64; DROP TABLE o_gcn_uint64;

DROP TABLE IF EXISTS c_gcn_int8; DROP TABLE IF EXISTS o_gcn_int8;
CREATE TABLE c_gcn_int8 (kt Tuple(Nullable(Int8), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_int8 (kt Tuple(Nullable(Int8), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_int8 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_int8 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid N Int8/UInt8' AS c1,
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid N Int8/UInt16' AS c1,
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid N Int8/UInt32' AS c1,
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid N Int8/UInt64' AS c1,
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid N Int8/Int8' AS c1,
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid N Int8/Int16' AS c1,
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid N Int8/Int32' AS c1,
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid N Int8/Int64' AS c1,
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gcn_int8; DROP TABLE o_gcn_int8;

DROP TABLE IF EXISTS c_gcn_int16; DROP TABLE IF EXISTS o_gcn_int16;
CREATE TABLE c_gcn_int16 (kt Tuple(Nullable(Int16), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_int16 (kt Tuple(Nullable(Int16), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_int16 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_int16 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid N Int16/UInt8' AS c1,
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid N Int16/UInt16' AS c1,
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid N Int16/UInt32' AS c1,
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid N Int16/UInt64' AS c1,
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid N Int16/Int8' AS c1,
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid N Int16/Int16' AS c1,
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid N Int16/Int32' AS c1,
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid N Int16/Int64' AS c1,
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gcn_int16; DROP TABLE o_gcn_int16;

DROP TABLE IF EXISTS c_gcn_int32; DROP TABLE IF EXISTS o_gcn_int32;
CREATE TABLE c_gcn_int32 (kt Tuple(Nullable(Int32), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_int32 (kt Tuple(Nullable(Int32), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_int32 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_int32 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid N Int32/UInt8' AS c1,
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid N Int32/UInt16' AS c1,
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid N Int32/UInt32' AS c1,
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid N Int32/UInt64' AS c1,
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid N Int32/Int8' AS c1,
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid N Int32/Int16' AS c1,
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid N Int32/Int32' AS c1,
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid N Int32/Int64' AS c1,
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gcn_int32; DROP TABLE o_gcn_int32;

DROP TABLE IF EXISTS c_gcn_int64; DROP TABLE IF EXISTS o_gcn_int64;
CREATE TABLE c_gcn_int64 (kt Tuple(Nullable(Int64), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_int64 (kt Tuple(Nullable(Int64), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_int64 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_int64 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid N Int64/UInt8' AS c1,
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid N Int64/UInt16' AS c1,
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid N Int64/UInt32' AS c1,
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid N Int64/UInt64' AS c1,
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid N Int64/Int8' AS c1,
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid N Int64/Int16' AS c1,
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid N Int64/Int32' AS c1,
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid N Int64/Int64' AS c1,
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gcn_int64; DROP TABLE o_gcn_int64;

SELECT '--- 12x12 integer cross product: every pair stays exact ---';
-- 12x12 integer cross product (arm 2): every pair must stay EXACT.
-- Generated; do not thin to a 'representative' subset.

DROP TABLE IF EXISTS ai_uint8; DROP TABLE IF EXISTS ao_uint8;
CREATE TABLE ai_uint8 (k UInt8) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint8 (k UInt8) ENGINE = Memory;
INSERT INTO ai_uint8 VALUES (1), (2);
INSERT INTO ao_uint8 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 UInt8/UInt8' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 UInt8/UInt16' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 UInt8/UInt32' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 UInt8/UInt64' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 UInt8/UInt128' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 UInt8/UInt256' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 UInt8/Int8' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 UInt8/Int16' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 UInt8/Int32' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 UInt8/Int64' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 UInt8/Int128' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 UInt8/Int256' AS c1,
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_uint8; DROP TABLE ao_uint8;

DROP TABLE IF EXISTS ai_uint16; DROP TABLE IF EXISTS ao_uint16;
CREATE TABLE ai_uint16 (k UInt16) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint16 (k UInt16) ENGINE = Memory;
INSERT INTO ai_uint16 VALUES (1), (2);
INSERT INTO ao_uint16 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 UInt16/UInt8' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 UInt16/UInt16' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 UInt16/UInt32' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 UInt16/UInt64' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 UInt16/UInt128' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 UInt16/UInt256' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 UInt16/Int8' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 UInt16/Int16' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 UInt16/Int32' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 UInt16/Int64' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 UInt16/Int128' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 UInt16/Int256' AS c1,
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_uint16; DROP TABLE ao_uint16;
