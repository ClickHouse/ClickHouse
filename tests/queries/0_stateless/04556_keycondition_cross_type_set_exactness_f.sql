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

-- 8x8 packed integer composites, plain and Nullable. Generated; do not thin.

DROP TABLE IF EXISTS c_gc_uint32; DROP TABLE IF EXISTS o_gc_uint32;
CREATE TABLE c_gc_uint32 (kt Tuple(UInt32, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_uint32 (kt Tuple(UInt32, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_uint32 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_uint32 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid P UInt32/UInt8' AS c1,
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid P UInt32/UInt16' AS c1,
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid P UInt32/UInt32' AS c1,
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid P UInt32/UInt64' AS c1,
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid P UInt32/Int8' AS c1,
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid P UInt32/Int16' AS c1,
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid P UInt32/Int32' AS c1,
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid P UInt32/Int64' AS c1,
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gc_uint32; DROP TABLE o_gc_uint32;

DROP TABLE IF EXISTS c_gc_uint64; DROP TABLE IF EXISTS o_gc_uint64;
CREATE TABLE c_gc_uint64 (kt Tuple(UInt64, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_uint64 (kt Tuple(UInt64, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_uint64 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_uint64 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid P UInt64/UInt8' AS c1,
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid P UInt64/UInt16' AS c1,
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid P UInt64/UInt32' AS c1,
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid P UInt64/UInt64' AS c1,
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid P UInt64/Int8' AS c1,
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid P UInt64/Int16' AS c1,
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid P UInt64/Int32' AS c1,
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid P UInt64/Int64' AS c1,
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gc_uint64; DROP TABLE o_gc_uint64;

DROP TABLE IF EXISTS c_gc_int8; DROP TABLE IF EXISTS o_gc_int8;
CREATE TABLE c_gc_int8 (kt Tuple(Int8, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_int8 (kt Tuple(Int8, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_int8 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_int8 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid P Int8/UInt8' AS c1,
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid P Int8/UInt16' AS c1,
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid P Int8/UInt32' AS c1,
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid P Int8/UInt64' AS c1,
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid P Int8/Int8' AS c1,
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid P Int8/Int16' AS c1,
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid P Int8/Int32' AS c1,
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid P Int8/Int64' AS c1,
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gc_int8; DROP TABLE o_gc_int8;

DROP TABLE IF EXISTS c_gc_int16; DROP TABLE IF EXISTS o_gc_int16;
CREATE TABLE c_gc_int16 (kt Tuple(Int16, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_int16 (kt Tuple(Int16, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_int16 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_int16 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid P Int16/UInt8' AS c1,
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid P Int16/UInt16' AS c1,
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid P Int16/UInt32' AS c1,
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid P Int16/UInt64' AS c1,
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid P Int16/Int8' AS c1,
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid P Int16/Int16' AS c1,
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid P Int16/Int32' AS c1,
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid P Int16/Int64' AS c1,
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gc_int16; DROP TABLE o_gc_int16;

DROP TABLE IF EXISTS c_gc_int32; DROP TABLE IF EXISTS o_gc_int32;
CREATE TABLE c_gc_int32 (kt Tuple(Int32, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_int32 (kt Tuple(Int32, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_int32 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_int32 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid P Int32/UInt8' AS c1,
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid P Int32/UInt16' AS c1,
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid P Int32/UInt32' AS c1,
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid P Int32/UInt64' AS c1,
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid P Int32/Int8' AS c1,
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid P Int32/Int16' AS c1,
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid P Int32/Int32' AS c1,
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid P Int32/Int64' AS c1,
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gc_int32; DROP TABLE o_gc_int32;

DROP TABLE IF EXISTS c_gc_int64; DROP TABLE IF EXISTS o_gc_int64;
CREATE TABLE c_gc_int64 (kt Tuple(Int64, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_int64 (kt Tuple(Int64, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_int64 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_int64 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid P Int64/UInt8' AS c1,
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid P Int64/UInt16' AS c1,
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid P Int64/UInt32' AS c1,
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid P Int64/UInt64' AS c1,
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid P Int64/Int8' AS c1,
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid P Int64/Int16' AS c1,
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid P Int64/Int32' AS c1,
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid P Int64/Int64' AS c1,
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gc_int64; DROP TABLE o_gc_int64;

DROP TABLE IF EXISTS c_gcn_uint8; DROP TABLE IF EXISTS o_gcn_uint8;
CREATE TABLE c_gcn_uint8 (kt Tuple(Nullable(UInt8), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_uint8 (kt Tuple(Nullable(UInt8), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_uint8 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_uint8 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid N UInt8/UInt8' AS c1,
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid N UInt8/UInt16' AS c1,
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid N UInt8/UInt32' AS c1,
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid N UInt8/UInt64' AS c1,
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid N UInt8/Int8' AS c1,
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid N UInt8/Int16' AS c1,
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid N UInt8/Int32' AS c1,
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid N UInt8/Int64' AS c1,
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gcn_uint8; DROP TABLE o_gcn_uint8;

DROP TABLE IF EXISTS c_gcn_uint16; DROP TABLE IF EXISTS o_gcn_uint16;
CREATE TABLE c_gcn_uint16 (kt Tuple(Nullable(UInt16), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_uint16 (kt Tuple(Nullable(UInt16), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_uint16 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_uint16 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid N UInt16/UInt8' AS c1,
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid N UInt16/UInt16' AS c1,
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid N UInt16/UInt32' AS c1,
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid N UInt16/UInt64' AS c1,
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid N UInt16/Int8' AS c1,
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid N UInt16/Int16' AS c1,
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid N UInt16/Int32' AS c1,
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid N UInt16/Int64' AS c1,
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gcn_uint16; DROP TABLE o_gcn_uint16;

DROP TABLE IF EXISTS c_gcn_uint32; DROP TABLE IF EXISTS o_gcn_uint32;
