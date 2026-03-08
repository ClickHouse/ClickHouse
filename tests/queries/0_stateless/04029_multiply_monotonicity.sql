-- Test that multiply by a constant is recognized as monotonic for
-- primary key condition analysis (`KeyCondition`), enabling index pruning
-- for WHERE clauses involving expressions like `key * C`.
--
-- When the known range [left, right] doesn't overflow under multiplication,
-- the function is monotonic and `KeyCondition` can transform the range.
-- When it DOES overflow, monotonicity must be rejected to avoid wrong pruning.

-- ============================================================
-- Part 1: Non-overflowing multiply — primary key pruning works
-- ============================================================

SET force_primary_key = 1; -- All tests run with force_primary_key to verify index usage.

DROP TABLE IF EXISTS t_multiply_mono;
CREATE TABLE t_multiply_mono (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_mono SELECT number FROM numbers(1000);

-- Small values, key * 2 doesn't overflow UInt64.
SELECT count() FROM t_multiply_mono WHERE key * 2 > 100;

DROP TABLE IF EXISTS t_multiply_mono_int;
CREATE TABLE t_multiply_mono_int (key Int64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_mono_int SELECT number FROM numbers(1000);

SELECT count() FROM t_multiply_mono_int WHERE key * 3 > 150;

-- ============================================================
-- Part 2: Overflowing multiply — must reject monotonicity
-- ============================================================

DROP TABLE IF EXISTS t_multiply_overflow;
CREATE TABLE t_multiply_overflow (key UInt64) ENGINE = MergeTree ORDER BY key;

-- 12297829382473034410 * 3 overflows UInt64.
INSERT INTO t_multiply_overflow VALUES (10), (12297829382473034410);

SELECT key FROM t_multiply_overflow WHERE key * 3 > 20 ORDER BY key;

-- Int64 overflow: -3074457345618258603 * 3 < Int64 min.
DROP TABLE IF EXISTS t_multiply_overflow_signed;
CREATE TABLE t_multiply_overflow_signed (key Int64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_overflow_signed VALUES (-3074457345618258603), (100);

SELECT key FROM t_multiply_overflow_signed WHERE key * 3 > 0 ORDER BY key;

-- ============================================================
-- Part 3: Widened types — overflow impossible
-- ============================================================

DROP TABLE IF EXISTS t_multiply_widened;
CREATE TABLE t_multiply_widened (key UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_widened SELECT number FROM numbers(1000);

-- UInt32 * UInt32 → UInt64 (widened), overflow impossible.
SELECT count() FROM t_multiply_widened WHERE key * 100 > 5000;

-- ============================================================
-- Part 4: Negative constant (not -1) — monotonically decreasing
-- ============================================================

DROP TABLE IF EXISTS t_multiply_neg;
CREATE TABLE t_multiply_neg (key Int64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_neg SELECT number FROM numbers(1000);

SELECT count() FROM t_multiply_neg WHERE key * -2 < -1000;

-- ============================================================
-- Part 5: UInt128 — non-overflowing (pruning works)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_u128;
CREATE TABLE t_multiply_u128 (key UInt128) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_u128 SELECT number FROM numbers(1000);

-- Small values, key * 2 doesn't overflow UInt128.
SELECT count() FROM t_multiply_u128 WHERE key * toUInt128(2) > 100;

-- ============================================================
-- Part 6: UInt128 — overflowing (pruning rejected)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_u128_overflow;
CREATE TABLE t_multiply_u128_overflow (key UInt128) ENGINE = MergeTree ORDER BY key;

-- Max UInt128 / 3 ≈ 1.13e38. This value * 3 overflows UInt128.
INSERT INTO t_multiply_u128_overflow VALUES (10), (170141183460469231731687303715884105726);

SELECT key FROM t_multiply_u128_overflow WHERE key * toUInt128(3) > 20 ORDER BY key;

-- ============================================================
-- Part 7: Int128 — non-overflowing (pruning works)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_i128;
CREATE TABLE t_multiply_i128 (key Int128) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_i128 SELECT number FROM numbers(1000);

SELECT count() FROM t_multiply_i128 WHERE key * toInt128(3) > 150;

-- ============================================================
-- Part 8: Int128 — overflowing (pruning rejected)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_i128_overflow;
CREATE TABLE t_multiply_i128_overflow (key Int128) ENGINE = MergeTree ORDER BY key;

-- -56713727820156410577229101238628035243 * 3 overflows Int128 (< Int128 min).
INSERT INTO t_multiply_i128_overflow VALUES (-56713727820156410577229101238628035243), (100);

SELECT key FROM t_multiply_i128_overflow WHERE key * toInt128(3) > 0 ORDER BY key;

-- ============================================================
-- Part 9: UInt256 — non-overflowing (pruning works)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_u256;
CREATE TABLE t_multiply_u256 (key UInt256) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_u256 SELECT number FROM numbers(1000);

SELECT count() FROM t_multiply_u256 WHERE key * toUInt256(5) > 500;

-- ============================================================
-- Part 10: UInt256 — overflowing (pruning rejected)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_u256_overflow;
CREATE TABLE t_multiply_u256_overflow (key UInt256) ENGINE = MergeTree ORDER BY key;

-- Max UInt256 / 3 ≈ 3.86e76. This value * 3 overflows UInt256.
INSERT INTO t_multiply_u256_overflow VALUES (10), (38597363079105398474523661669562635951089994888546854679819194669304376546646);

SELECT key FROM t_multiply_u256_overflow WHERE key * toUInt256(3) > 20 ORDER BY key;

-- ============================================================
-- Part 11: Int256 — non-overflowing (pruning works)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_i256;
CREATE TABLE t_multiply_i256 (key Int256) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_i256 SELECT number FROM numbers(1000);

SELECT count() FROM t_multiply_i256 WHERE key * toInt256(4) > 400;

-- ============================================================
-- Part 12: Int256 — overflowing (pruning rejected)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_i256_overflow;
CREATE TABLE t_multiply_i256_overflow (key Int256) ENGINE = MergeTree ORDER BY key;

-- -19298681539552699237261830834781317975544997444273427339909597334652188273324 * 3 overflows Int256.
INSERT INTO t_multiply_i256_overflow VALUES (-19298681539552699237261830834781317975544997444273427339909597334652188273324), (100);

SELECT key FROM t_multiply_i256_overflow WHERE key * toInt256(3) > 0 ORDER BY key;

-- ============================================================
-- Part 13: Multiply by zero — constant result, trivially monotonic
-- ============================================================

DROP TABLE IF EXISTS t_multiply_zero;
CREATE TABLE t_multiply_zero (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_zero SELECT number FROM numbers(1000);

SELECT count() FROM t_multiply_zero WHERE key * 0 = 0;

-- ============================================================
-- Part 14: Constant on the left side — `C * key`
-- ============================================================

DROP TABLE IF EXISTS t_multiply_left_const;
CREATE TABLE t_multiply_left_const (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_left_const SELECT number FROM numbers(1000);

SELECT count() FROM t_multiply_left_const WHERE 2 * key > 100;

-- ============================================================
-- Part 15: Multiply by 1 — identity
-- ============================================================

DROP TABLE IF EXISTS t_multiply_one;
CREATE TABLE t_multiply_one (key Int64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_one SELECT number FROM numbers(1000);

SELECT count() FROM t_multiply_one WHERE key * 1 > 500;

-- ============================================================
-- Part 16: key * -1 with INT64_MIN — signed division UB edge case
-- ============================================================

DROP TABLE IF EXISTS t_multiply_neg1_min;
CREATE TABLE t_multiply_neg1_min (key Int64) ENGINE = MergeTree ORDER BY key;

-- INT64_MIN * -1 overflows Int64 (result would be INT64_MAX + 1).
INSERT INTO t_multiply_neg1_min VALUES (-9223372036854775808), (100);

SELECT key FROM t_multiply_neg1_min WHERE key * -1 < -99 ORDER BY key;

-- ============================================================
-- Part 17: Negative constant with overflow
-- ============================================================

DROP TABLE IF EXISTS t_multiply_neg_overflow;
CREATE TABLE t_multiply_neg_overflow (key Int64) ENGINE = MergeTree ORDER BY key;

-- 4611686018427387904 * -3 = -13835058055282163712 which overflows Int64 (< Int64 min).
INSERT INTO t_multiply_neg_overflow VALUES (4611686018427387904), (10);

SELECT key FROM t_multiply_neg_overflow WHERE key * -3 < 0 ORDER BY key;

-- ============================================================
-- Part 18: variable * variable — no constant, cannot use primary key
-- ============================================================

DROP TABLE IF EXISTS t_multiply_var_var;
CREATE TABLE t_multiply_var_var (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_var_var SELECT number, number + 1 FROM numbers(1000);

SELECT count() FROM t_multiply_var_var WHERE key * val > 500; -- { serverError INDEX_NOT_USED }

-- ============================================================
-- Part 19: key * -1 without overflow — decreasing monotonicity works
-- ============================================================

DROP TABLE IF EXISTS t_multiply_neg1_ok;
CREATE TABLE t_multiply_neg1_ok (key Int64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_multiply_neg1_ok SELECT number FROM numbers(1000);

-- No INT64_MIN in range, so key * -1 doesn't overflow.
SELECT count() FROM t_multiply_neg1_ok WHERE key * -1 < -500;

-- ============================================================
-- Part 20: Boundary — endpoint exactly at TYPE_MAX / c (no overflow)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_boundary;
CREATE TABLE t_multiply_boundary (key UInt64) ENGINE = MergeTree ORDER BY key;

-- TYPE_MAX / 3 = 6148914691236517205. key * 3 = 18446744073709551615 = TYPE_MAX exactly.
INSERT INTO t_multiply_boundary SELECT number FROM numbers(100);
INSERT INTO t_multiply_boundary VALUES (6148914691236517205);

SELECT count() FROM t_multiply_boundary WHERE key * 3 > 0;

-- ============================================================
-- Cleanup
-- ============================================================

DROP TABLE t_multiply_mono;
DROP TABLE t_multiply_mono_int;
DROP TABLE t_multiply_overflow;
DROP TABLE t_multiply_overflow_signed;
DROP TABLE t_multiply_widened;
DROP TABLE t_multiply_neg;
DROP TABLE t_multiply_u128;
DROP TABLE t_multiply_u128_overflow;
DROP TABLE t_multiply_i128;
DROP TABLE t_multiply_i128_overflow;
DROP TABLE t_multiply_u256;
DROP TABLE t_multiply_u256_overflow;
DROP TABLE t_multiply_i256;
DROP TABLE t_multiply_i256_overflow;
DROP TABLE t_multiply_zero;
DROP TABLE t_multiply_left_const;
DROP TABLE t_multiply_one;
DROP TABLE t_multiply_neg1_min;
DROP TABLE t_multiply_neg_overflow;
DROP TABLE t_multiply_var_var;
DROP TABLE t_multiply_neg1_ok;
DROP TABLE t_multiply_boundary;
