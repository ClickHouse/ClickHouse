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

SELECT '--- 12x12 integer cross product: every pair stays exact ---';
CREATE TABLE ai_int16 (k Int16) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int16 (k Int16) ENGINE = Memory;
INSERT INTO ai_int16 VALUES (1), (2);
INSERT INTO ao_int16 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 Int16/UInt8' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 Int16/UInt16' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 Int16/UInt32' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 Int16/UInt64' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 Int16/UInt128' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 Int16/UInt256' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 Int16/Int8' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 Int16/Int16' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 Int16/Int32' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 Int16/Int64' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 Int16/Int128' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 Int16/Int256' AS c1,
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_int16; DROP TABLE ao_int16;

DROP TABLE IF EXISTS ai_int32; DROP TABLE IF EXISTS ao_int32;
CREATE TABLE ai_int32 (k Int32) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int32 (k Int32) ENGINE = Memory;
INSERT INTO ai_int32 VALUES (1), (2);
INSERT INTO ao_int32 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 Int32/UInt8' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 Int32/UInt16' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 Int32/UInt32' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 Int32/UInt64' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 Int32/UInt128' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 Int32/UInt256' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 Int32/Int8' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 Int32/Int16' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 Int32/Int32' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 Int32/Int64' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 Int32/Int128' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 Int32/Int256' AS c1,
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_int32; DROP TABLE ao_int32;

DROP TABLE IF EXISTS ai_int64; DROP TABLE IF EXISTS ao_int64;
CREATE TABLE ai_int64 (k Int64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int64 (k Int64) ENGINE = Memory;
INSERT INTO ai_int64 VALUES (1), (2);
INSERT INTO ao_int64 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 Int64/UInt8' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 Int64/UInt16' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 Int64/UInt32' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 Int64/UInt64' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 Int64/UInt128' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 Int64/UInt256' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 Int64/Int8' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 Int64/Int16' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 Int64/Int32' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 Int64/Int64' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 Int64/Int128' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 Int64/Int256' AS c1,
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_int64; DROP TABLE ao_int64;

DROP TABLE IF EXISTS ai_int128; DROP TABLE IF EXISTS ao_int128;
CREATE TABLE ai_int128 (k Int128) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int128 (k Int128) ENGINE = Memory;
INSERT INTO ai_int128 VALUES (1), (2);
INSERT INTO ao_int128 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 Int128/UInt8' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 Int128/UInt16' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 Int128/UInt32' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 Int128/UInt64' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 Int128/UInt128' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 Int128/UInt256' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 Int128/Int8' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 Int128/Int16' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 Int128/Int32' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 Int128/Int64' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 Int128/Int128' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 Int128/Int256' AS c1,
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_int128; DROP TABLE ao_int128;

DROP TABLE IF EXISTS ai_int256; DROP TABLE IF EXISTS ao_int256;
CREATE TABLE ai_int256 (k Int256) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int256 (k Int256) ENGINE = Memory;
INSERT INTO ai_int256 VALUES (1), (2);
INSERT INTO ao_int256 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 Int256/UInt8' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 Int256/UInt16' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 Int256/UInt32' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 Int256/UInt64' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 Int256/UInt128' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 Int256/UInt256' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 Int256/Int8' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 Int256/Int16' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 Int256/Int32' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 Int256/Int64' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 Int256/Int128' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 Int256/Int256' AS c1,
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_int256; DROP TABLE ao_int256;

SELECT '--- attribute axis: parameters that `equals` treats as interchangeable stay exact ---';

-- `IDataType::equals` ignores the time zone of `DateTime`/`DateTime64` and the precision of
-- `Decimal`, while `getName` prints all three. Deciding exactness by name would decline these
-- pairs and silently lose pruning for the very common shape of a key that declares a time zone
-- against a set element that does not. Each pair below must keep its atom, and the neighbouring
-- pair that differs in a parameter `equals` DOES compare must still decline.

DROP TABLE IF EXISTS at_dt; DROP TABLE IF EXISTS ao_dt;
CREATE TABLE at_dt (t DateTime('UTC')) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
CREATE TABLE ao_dt (t DateTime('UTC')) ENGINE = Memory;
INSERT INTO at_dt VALUES ('2024-01-01 00:00:00'), ('2024-01-02 00:00:00'), ('2024-01-03 00:00:00');
INSERT INTO ao_dt VALUES ('2024-01-01 00:00:00'), ('2024-01-02 00:00:00'), ('2024-01-03 00:00:00');
SELECT 'attr DateTime(UTC)/DateTime prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr DateTime(UTC)/DateTime',
    (SELECT count() FROM at_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00'))) = (SELECT count() FROM ao_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00'))),
    (SELECT count() FROM at_dt WHERE t NOT IN (SELECT toDateTime('2024-01-01 00:00:00'))) = (SELECT count() FROM ao_dt WHERE t NOT IN (SELECT toDateTime('2024-01-01 00:00:00')));
SELECT 'attr DateTime(UTC)/DateTime(Asia/Istanbul) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr DateTime(UTC)/DateTime(Asia/Istanbul)',
    (SELECT count() FROM at_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul'))) = (SELECT count() FROM ao_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul'))),
    (SELECT count() FROM at_dt WHERE t NOT IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul'))) = (SELECT count() FROM ao_dt WHERE t NOT IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul')));
DROP TABLE at_dt; DROP TABLE ao_dt;

DROP TABLE IF EXISTS at_dt64; DROP TABLE IF EXISTS ao_dt64;
CREATE TABLE at_dt64 (t DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
CREATE TABLE ao_dt64 (t DateTime64(3, 'UTC')) ENGINE = Memory;
INSERT INTO at_dt64 VALUES ('2024-01-01 00:00:00.000'), ('2024-01-02 00:00:00.000'), ('2024-01-03 00:00:00.000');
INSERT INTO ao_dt64 VALUES ('2024-01-01 00:00:00.000'), ('2024-01-02 00:00:00.000'), ('2024-01-03 00:00:00.000');
SELECT 'attr DateTime64(3,UTC)/DateTime64(3) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dt64 WHERE t IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr DateTime64(3,UTC)/DateTime64(3)',
    (SELECT count() FROM at_dt64 WHERE t IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)'))) = (SELECT count() FROM ao_dt64 WHERE t IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)'))),
    (SELECT count() FROM at_dt64 WHERE t NOT IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)'))) = (SELECT count() FROM ao_dt64 WHERE t NOT IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)')));
-- scale IS compared by `equals`, so a cross-scale pair must still decline (the axis is not widened).
SELECT 'attr DateTime64(3,UTC)/DateTime64(6) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dt64 WHERE t IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(6)'))) WHERE explain ILIKE '%in 1-element set%';
DROP TABLE at_dt64; DROP TABLE ao_dt64;

DROP TABLE IF EXISTS at_dec; DROP TABLE IF EXISTS ao_dec;
CREATE TABLE at_dec (d Decimal(10, 2)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
CREATE TABLE ao_dec (d Decimal(10, 2)) ENGINE = Memory;
INSERT INTO at_dec VALUES (1.00), (2.00), (3.00);
INSERT INTO ao_dec VALUES (1.00), (2.00), (3.00);
SELECT 'attr Decimal(10,2)/Decimal(18,2) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dec WHERE d IN (SELECT CAST('1.00', 'Decimal(18,2)'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr Decimal(10,2)/Decimal(18,2)',
    (SELECT count() FROM at_dec WHERE d IN (SELECT CAST('1.00', 'Decimal(18,2)'))) = (SELECT count() FROM ao_dec WHERE d IN (SELECT CAST('1.00', 'Decimal(18,2)'))),
    (SELECT count() FROM at_dec WHERE d NOT IN (SELECT CAST('1.00', 'Decimal(18,2)'))) = (SELECT count() FROM ao_dec WHERE d NOT IN (SELECT CAST('1.00', 'Decimal(18,2)')));
-- `Decimal(20,2)` is a `Decimal128` while `Decimal(10,2)` is a `Decimal64`, so `equals` is false on
-- the differing underlying type and the pair must decline even though only the precision is written.
SELECT 'attr Decimal(10,2)/Decimal(20,2) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dec WHERE d IN (SELECT CAST('1.00', 'Decimal(20,2)'))) WHERE explain ILIKE '%in 1-element set%';
DROP TABLE at_dec; DROP TABLE ao_dec;

DROP TABLE IF EXISTS at_t64;
CREATE TABLE at_t64 (t Time64(3)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
INSERT INTO at_t64 VALUES ('12:00:00.123'), ('13:00:00.000'), ('14:00:00.000');
SELECT 'attr Time64(3)/Time64(3) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_t64 WHERE t IN (SELECT CAST('12:00:00.123', 'Time64(3)'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr Time64(3)/Time64(6) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_t64 WHERE t IN (SELECT CAST('12:00:00.123', 'Time64(6)'))) WHERE explain ILIKE '%in 1-element set%';
DROP TABLE at_t64;

-- `Bool` is a `DataTypeUInt8` carrying a custom name, so it is `equals`-equal to a plain `UInt8`
-- while its cast wrapper clamps every nonzero value to 1. That is not equality-preserving, so a
-- custom name must still decline in both directions -- and the `UInt8` key direction is a genuine
-- wrong-results carrier, not just a pruning question.
DROP TABLE IF EXISTS at_bool;
CREATE TABLE at_bool (b Bool) ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 1;
INSERT INTO at_bool VALUES (false), (true);
SELECT 'attr Bool/UInt8 declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_bool WHERE b IN (SELECT toUInt8(1))) WHERE explain ILIKE '%in 1-element set%';
DROP TABLE at_bool;

DROP TABLE IF EXISTS at_u8; DROP TABLE IF EXISTS ao_u8;
CREATE TABLE at_u8 (b UInt8) ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 1;
CREATE TABLE ao_u8 (b UInt8) ENGINE = Memory;
INSERT INTO at_u8 VALUES (0), (1), (7);
INSERT INTO ao_u8 VALUES (0), (1), (7);
SELECT 'attr UInt8/Bool declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_u8 WHERE b IN (SELECT CAST(1, 'Bool'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr UInt8/Bool',
    (SELECT count() FROM at_u8 WHERE b IN (SELECT CAST(1, 'Bool'))) = (SELECT count() FROM ao_u8 WHERE b IN (SELECT CAST(1, 'Bool'))),
    (SELECT count() FROM at_u8 WHERE b NOT IN (SELECT CAST(1, 'Bool'))) = (SELECT count() FROM ao_u8 WHERE b NOT IN (SELECT CAST(1, 'Bool')));
DROP TABLE at_u8; DROP TABLE ao_u8;

-- The custom-name check has to recurse, because `Tuple(Bool, UInt8)` and `Tuple(UInt8, UInt8)`
-- differ only in a NESTED custom name and container `equals` compares elements with `equals`.

DROP TABLE IF EXISTS at_tb; DROP TABLE IF EXISTS ao_tb;
CREATE TABLE at_tb (t Tuple(Bool, UInt8)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
CREATE TABLE ao_tb (t Tuple(Bool, UInt8)) ENGINE = Memory;
INSERT INTO at_tb VALUES ((true, 1)), ((false, 2)), ((true, 3));
INSERT INTO ao_tb VALUES ((true, 1)), ((false, 2)), ((true, 3));
SELECT 'attr Tuple(Bool,UInt8)/Tuple(UInt8,UInt8) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_tb WHERE t IN (SELECT tuple(toUInt8(1), toUInt8(1)))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr Tuple(Bool,UInt8)/Tuple(UInt8,UInt8)',
    (SELECT count() FROM at_tb WHERE t IN (SELECT tuple(toUInt8(1), toUInt8(1)))) = (SELECT count() FROM ao_tb WHERE t IN (SELECT tuple(toUInt8(1), toUInt8(1))));
DROP TABLE at_tb; DROP TABLE ao_tb;

-- A `Dynamic` element is judged by the types it holds, not by `Dynamic` itself. `has` compares `Field`s,
-- so a stored type that converts losslessly into the key keeps the index; `IN` casts the key into the
-- declared `Dynamic` and so keeps the key's own concrete type, where `UInt8(1)` and `UInt64(1)` are two
-- different values. The `IN` cells therefore have to decline where the `has` cells prune.

DROP TABLE IF EXISTS at_dyn; DROP TABLE IF EXISTS ao_dyn;
CREATE TABLE at_dyn (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k SETTINGS index_granularity = 1;
CREATE TABLE ao_dyn (k UInt64) ENGINE = Memory;
INSERT INTO at_dyn VALUES (1), (2);
INSERT INTO ao_dyn VALUES (1), (2);
SELECT 'dyn UInt64/Dynamic(UInt8) IN declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dyn WHERE k IN (SELECT arrayJoin(CAST([toUInt8(1)], 'Array(Dynamic)')))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'dyn UInt64/Dynamic(UInt8) IN',
    (SELECT count() FROM at_dyn WHERE k IN (SELECT arrayJoin(CAST([toUInt8(1)], 'Array(Dynamic)')))) = (SELECT count() FROM ao_dyn WHERE k IN (SELECT arrayJoin(CAST([toUInt8(1)], 'Array(Dynamic)')))),
    (SELECT count() FROM at_dyn WHERE k NOT IN (SELECT arrayJoin(CAST([toUInt8(1)], 'Array(Dynamic)')))) = (SELECT count() FROM ao_dyn WHERE k NOT IN (SELECT arrayJoin(CAST([toUInt8(1)], 'Array(Dynamic)'))));
-- The same element under `has` keeps the index, which is what makes the decline above specific to `IN`
-- rather than a blanket refusal of every `Dynamic`. Both polarities assert an exact part reduction: a
-- relaxed atom is still printed and still prunes positively, but it forces `can_be_false`, so only the
-- negated form distinguishes an exact atom from a relaxed one.
SELECT 'dyn UInt64/Dynamic(UInt8) has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dyn WHERE has(CAST([toUInt8(1)], 'Array(Dynamic)'), k)) WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'dyn UInt64/Dynamic(UInt8) notHas prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dyn WHERE notHas(CAST([toUInt8(1)], 'Array(Dynamic)'), k)) WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'dyn UInt64/Dynamic(UInt8) has',
    (SELECT count() FROM at_dyn WHERE has(CAST([toUInt8(1)], 'Array(Dynamic)'), k)) = (SELECT count() FROM ao_dyn WHERE has(CAST([toUInt8(1)], 'Array(Dynamic)'), k)),
    (SELECT count() FROM at_dyn WHERE notHas(CAST([toUInt8(1)], 'Array(Dynamic)'), k)) = (SELECT count() FROM ao_dyn WHERE notHas(CAST([toUInt8(1)], 'Array(Dynamic)'), k));
DROP TABLE at_dyn; DROP TABLE ao_dyn;

-- A `Dynamic` holding a `Float64` cannot keep a `String` key's index even under `has`: the element casts
-- to the single value '3' for the index while matching '3', '3.0' and '3.00' at runtime.
DROP TABLE IF EXISTS at_dyns; DROP TABLE IF EXISTS ao_dyns;
CREATE TABLE at_dyns (k String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE ao_dyns (k String) ENGINE = Memory;
INSERT INTO at_dyns VALUES ('3'), ('3.0'), ('3.00'), ('4');
INSERT INTO ao_dyns VALUES ('3'), ('3.0'), ('3.00'), ('4');
SELECT 'dyn String/Dynamic(Float64) has declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dyns WHERE has(CAST([3.0], 'Array(Dynamic)'), k)) WHERE explain ILIKE '%in 1-element set%';
-- The rows that only a loose comparison reaches: a Float64 element equals '3', '3.0' and '3.00' at
-- runtime while the index would hold the single value '3', so this is the wrong-results arm.
SELECT 'dyn String/Dynamic(Float64) has reads all', (SELECT count() FROM at_dyns WHERE has(CAST([3.0], 'Array(Dynamic)'), k)) = 3;
SELECT 'dyn String/Dynamic(Float64) has',
    (SELECT count() FROM at_dyns WHERE has(CAST([3.0], 'Array(Dynamic)'), k)) = (SELECT count() FROM ao_dyns WHERE has(CAST([3.0], 'Array(Dynamic)'), k)),
    (SELECT count() FROM at_dyns WHERE notHas(CAST([3.0], 'Array(Dynamic)'), k)) = (SELECT count() FROM ao_dyns WHERE notHas(CAST([3.0], 'Array(Dynamic)'), k));
-- A `String`-holding `Dynamic` compares exactly against the same key, so it keeps the index: the decline
-- above is driven by the type stored, not by `Dynamic`.
SELECT 'dyn String/Dynamic(String) has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dyns WHERE has(CAST(['3'], 'Array(Dynamic)'), k)) WHERE explain ILIKE '%Granules: 1/4%';
SELECT 'dyn String/Dynamic(String) has',
    (SELECT count() FROM at_dyns WHERE has(CAST(['3'], 'Array(Dynamic)'), k)) = (SELECT count() FROM ao_dyns WHERE has(CAST(['3'], 'Array(Dynamic)'), k)),
    (SELECT count() FROM at_dyns WHERE notHas(CAST(['3'], 'Array(Dynamic)'), k)) = (SELECT count() FROM ao_dyns WHERE notHas(CAST(['3'], 'Array(Dynamic)'), k));
-- One inexact type in a mixed set is enough to decline the whole element.
SELECT 'dyn String/Dynamic(String,Float64) has declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dyns WHERE has(CAST(['3', 3.0], 'Array(Dynamic)'), k)) WHERE explain ILIKE '%in 1-element set%';
SELECT 'dyn String/Dynamic(String,Float64) has',
    (SELECT count() FROM at_dyns WHERE has(CAST(['3', 3.0], 'Array(Dynamic)'), k)) = (SELECT count() FROM ao_dyns WHERE has(CAST(['3', 3.0], 'Array(Dynamic)'), k));
DROP TABLE at_dyns; DROP TABLE ao_dyns;

-- A plain `Variant` element stays declined where a `Dynamic` one is recovered, because the declared
-- alternative set makes the two directions disagree: index preparation casts `Variant(UInt8(3))` into
-- the key type and gets the `String` '3', while runtime membership casts the key into the `Variant`,
-- where a `String` key lands in the `String` arm and the element sits in the `UInt8` arm, so they never
-- compare equal. `Dynamic` declares no alternative set for the key to be cast into.
DROP TABLE IF EXISTS at_var; DROP TABLE IF EXISTS ao_var;
CREATE TABLE at_var (k String) ENGINE = MergeTree ORDER BY k PARTITION BY k SETTINGS index_granularity = 1;
CREATE TABLE ao_var (k String) ENGINE = Memory;
INSERT INTO at_var VALUES ('3');
INSERT INTO at_var VALUES ('4');
INSERT INTO ao_var VALUES ('3'), ('4');
SELECT 'var String/Variant(String,UInt8) IN declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_var WHERE k NOT IN (SELECT CAST(toUInt8(3), 'Variant(String, UInt8)'))) WHERE explain ILIKE '%1-element set%';
-- The wrong-results arm: an exact atom prunes the '3' part away while the runtime predicate keeps it.
SELECT 'var String/Variant(String,UInt8) IN',
    (SELECT count() FROM at_var WHERE k IN (SELECT CAST(toUInt8(3), 'Variant(String, UInt8)'))) = (SELECT count() FROM ao_var WHERE k IN (SELECT CAST(toUInt8(3), 'Variant(String, UInt8)'))),
    (SELECT count() FROM at_var WHERE k NOT IN (SELECT CAST(toUInt8(3), 'Variant(String, UInt8)'))) = (SELECT count() FROM ao_var WHERE k NOT IN (SELECT CAST(toUInt8(3), 'Variant(String, UInt8)')));
-- The keep-pruning control on the same fixture: a same-typed element still reduces the parts read, so
-- the decline above is specific to the `Variant` element rather than a property of this table.
SELECT 'var String/String IN prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_var WHERE k NOT IN (SELECT '3')) WHERE explain ILIKE '%Parts: 1/2%';
DROP TABLE at_var; DROP TABLE ao_var;
