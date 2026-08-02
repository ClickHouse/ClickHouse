-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

-- Part of the 04549/04552-04558 family: one set-index exactness suite split across files to fit
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
-- 12x12 integer cross product (arm 2): every pair must stay EXACT.
-- Generated; do not thin to a 'representative' subset.

DROP TABLE IF EXISTS ai_uint32; DROP TABLE IF EXISTS ao_uint32;
CREATE TABLE ai_uint32 (k UInt32) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint32 (k UInt32) ENGINE = Memory;
INSERT INTO ai_uint32 VALUES (1), (2);
INSERT INTO ao_uint32 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 UInt32/UInt8' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 UInt32/UInt16' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 UInt32/UInt32' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 UInt32/UInt64' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 UInt32/UInt128' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 UInt32/UInt256' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 UInt32/Int8' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 UInt32/Int16' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 UInt32/Int32' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 UInt32/Int64' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 UInt32/Int128' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 UInt32/Int256' AS c1,
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_uint32; DROP TABLE ao_uint32;

DROP TABLE IF EXISTS ai_uint64; DROP TABLE IF EXISTS ao_uint64;
CREATE TABLE ai_uint64 (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint64 (k UInt64) ENGINE = Memory;
INSERT INTO ai_uint64 VALUES (1), (2);
INSERT INTO ao_uint64 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 UInt64/UInt8' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 UInt64/UInt16' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 UInt64/UInt32' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 UInt64/UInt64' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 UInt64/UInt128' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 UInt64/UInt256' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 UInt64/Int8' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 UInt64/Int16' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 UInt64/Int32' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 UInt64/Int64' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 UInt64/Int128' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 UInt64/Int256' AS c1,
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_uint64; DROP TABLE ao_uint64;

DROP TABLE IF EXISTS ai_uint128; DROP TABLE IF EXISTS ao_uint128;
CREATE TABLE ai_uint128 (k UInt128) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint128 (k UInt128) ENGINE = Memory;
INSERT INTO ai_uint128 VALUES (1), (2);
INSERT INTO ao_uint128 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 UInt128/UInt8' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 UInt128/UInt16' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 UInt128/UInt32' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 UInt128/UInt64' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 UInt128/UInt128' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 UInt128/UInt256' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 UInt128/Int8' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 UInt128/Int16' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 UInt128/Int32' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 UInt128/Int64' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 UInt128/Int128' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 UInt128/Int256' AS c1,
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_uint128; DROP TABLE ao_uint128;

DROP TABLE IF EXISTS ai_uint256; DROP TABLE IF EXISTS ao_uint256;
CREATE TABLE ai_uint256 (k UInt256) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint256 (k UInt256) ENGINE = Memory;
INSERT INTO ai_uint256 VALUES (1), (2);
INSERT INTO ao_uint256 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 UInt256/UInt8' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 UInt256/UInt16' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 UInt256/UInt32' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 UInt256/UInt64' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 UInt256/UInt128' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 UInt256/UInt256' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 UInt256/Int8' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 UInt256/Int16' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 UInt256/Int32' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 UInt256/Int64' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 UInt256/Int128' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 UInt256/Int256' AS c1,
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_uint256; DROP TABLE ao_uint256;

DROP TABLE IF EXISTS ai_int8; DROP TABLE IF EXISTS ao_int8;
CREATE TABLE ai_int8 (k Int8) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int8 (k Int8) ENGINE = Memory;
INSERT INTO ai_int8 VALUES (1), (2);
INSERT INTO ao_int8 VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'arm2 Int8/UInt8' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt8(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt8(1))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'arm2 Int8/UInt16' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt16(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt16(1))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'arm2 Int8/UInt32' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt32(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt32(1))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'arm2 Int8/UInt64' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt64(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt64(1))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'arm2 Int8/UInt128' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt128(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt128(1))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'arm2 Int8/UInt256' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt256(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt256(1))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'arm2 Int8/Int8' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt8(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt8(1))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'arm2 Int8/Int16' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt16(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt16(1))) AS c3
    UNION ALL
    SELECT 9 AS ord, 'arm2 Int8/Int32' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt32(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt32(1))) AS c3
    UNION ALL
    SELECT 10 AS ord, 'arm2 Int8/Int64' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt64(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt64(1))) AS c3
    UNION ALL
    SELECT 11 AS ord, 'arm2 Int8/Int128' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt128(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt128(1))) AS c3
    UNION ALL
    SELECT 12 AS ord, 'arm2 Int8/Int256' AS c1,
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt256(1))) AS c2,
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt256(1))) AS c3
) ORDER BY ord;
DROP TABLE ai_int8; DROP TABLE ao_int8;

DROP TABLE IF EXISTS ai_int16; DROP TABLE IF EXISTS ao_int16;
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
