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
