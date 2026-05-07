-- Test: exercises GCD codec real compression path (gcd_multiplier > 1) for multiple data sizes.
-- Covers: src/Compression/CompressionCodecGCD.cpp:155-164 — decompressDataForType multiplication loop
--          and the new chassert(source == source_end) at line 164.
-- All existing tests for CODEC(GCD) either use Memory engine (codec not applied) or use
-- values with gcd in {0, 1} which take the special-case `memcpy` branch only.
-- The actual GCD compression path (divide on compress, multiply on decompress) had zero coverage.

DROP TABLE IF EXISTS gcd_real_u32;
DROP TABLE IF EXISTS gcd_real_u64;
DROP TABLE IF EXISTS gcd_real_u128;
DROP TABLE IF EXISTS gcd_real_u256;

-- UInt32: values are multiples of 7 → gcd = 7, exercises divide/multiply path for size 4
CREATE TABLE gcd_real_u32 (n UInt32 CODEC(GCD, LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO gcd_real_u32 SELECT number * 7 FROM numbers(10000);
OPTIMIZE TABLE gcd_real_u32 FINAL;
SELECT count(), sum(n), min(n), max(n), n % 7 = 0 AS divisible_by_7 FROM gcd_real_u32 GROUP BY divisible_by_7 ORDER BY divisible_by_7;

-- UInt64: values are multiples of 1000 → gcd = 1000, exercises divide/multiply path for size 8
CREATE TABLE gcd_real_u64 (n UInt64 CODEC(GCD, LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO gcd_real_u64 SELECT number * 1000 FROM numbers(10000);
OPTIMIZE TABLE gcd_real_u64 FINAL;
SELECT count(), sum(n), min(n), max(n), n % 1000 = 0 AS divisible_by_1000 FROM gcd_real_u64 GROUP BY divisible_by_1000 ORDER BY divisible_by_1000;

-- UInt128: values are multiples of 13 → gcd = 13, exercises divide/multiply path for size 16
CREATE TABLE gcd_real_u128 (n UInt128 CODEC(GCD, LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO gcd_real_u128 SELECT toUInt128(number) * 13 FROM numbers(10000);
OPTIMIZE TABLE gcd_real_u128 FINAL;
SELECT count(), sum(n), min(n), max(n), n % 13 = 0 AS divisible_by_13 FROM gcd_real_u128 GROUP BY divisible_by_13 ORDER BY divisible_by_13;

-- UInt256: values are multiples of 17 → gcd = 17, exercises divide/multiply path for size 32
CREATE TABLE gcd_real_u256 (n UInt256 CODEC(GCD, LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO gcd_real_u256 SELECT toUInt256(number) * 17 FROM numbers(10000);
OPTIMIZE TABLE gcd_real_u256 FINAL;
SELECT count(), sum(n), min(n), max(n), n % 17 = 0 AS divisible_by_17 FROM gcd_real_u256 GROUP BY divisible_by_17 ORDER BY divisible_by_17;

DROP TABLE gcd_real_u32;
DROP TABLE gcd_real_u64;
DROP TABLE gcd_real_u128;
DROP TABLE gcd_real_u256;
