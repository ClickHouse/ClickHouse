-- Test that `multiply` by a constant is recognized as monotonic for primary-key
-- condition analysis (`KeyCondition`), enabling index pruning for WHERE clauses
-- involving expressions like `key * C`.
--
-- Overflow detection: when the key range would overflow the result type,
-- monotonicity is correctly rejected so that pruning does not skip matching rows.
--
-- Uses `index_granularity = 1` so each row is a separate granule.
-- Verifies behavior via EXPLAIN indexes = 1 (Condition and Granules lines).

-- ============================================================
-- Part 1: UInt64 — pruning works
-- ============================================================

SET use_primary_key = 1;

DROP TABLE IF EXISTS t_multiply_mono;
CREATE TABLE t_multiply_mono (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_mono SELECT number FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_mono WHERE key * 2 > 10) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 2: Int64 — pruning works
-- ============================================================

DROP TABLE IF EXISTS t_multiply_mono_int;
CREATE TABLE t_multiply_mono_int (key Int64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_mono_int SELECT number FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_mono_int WHERE key * 3 > 15) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 3: UInt64 — overflow value present, overflow detected in
-- the granule whose range spans the overflow boundary; other
-- granules are pruned normally (4/11 kept, not 11/11)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_overflow;
CREATE TABLE t_multiply_overflow (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_overflow SELECT number FROM numbers(10);
-- 6148914691236517207 * 3 wraps to 5 in UInt64.
INSERT INTO t_multiply_overflow VALUES (6148914691236517207);
OPTIMIZE TABLE t_multiply_overflow FINAL;

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_overflow WHERE key * 3 > 20) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 4: Int64 — overflow, all granules kept (the overflow
-- value is at the start, affecting the widest granule range)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_overflow_signed;
CREATE TABLE t_multiply_overflow_signed (key Int64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_overflow_signed SELECT number FROM numbers(10);
-- -3074457345618258603 * 3 overflows Int64.
INSERT INTO t_multiply_overflow_signed VALUES (-3074457345618258603);
OPTIMIZE TABLE t_multiply_overflow_signed FINAL;

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_overflow_signed WHERE key * 3 > 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 5: UInt32 — widened to UInt64, overflow impossible
-- ============================================================

DROP TABLE IF EXISTS t_multiply_widened;
CREATE TABLE t_multiply_widened (key UInt32) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_widened SELECT number FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_widened WHERE key * 100 > 500) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 6: Negative constant — monotonically decreasing
-- ============================================================

DROP TABLE IF EXISTS t_multiply_neg;
CREATE TABLE t_multiply_neg (key Int64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_neg SELECT number FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_neg WHERE key * -2 < -10) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 7: UInt128 — pruning works
-- ============================================================

DROP TABLE IF EXISTS t_multiply_u128;
CREATE TABLE t_multiply_u128 (key UInt128) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_u128 SELECT number FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_u128 WHERE key * toUInt128(2) > 10) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 8: Int128 — overflow, all granules kept
-- ============================================================

DROP TABLE IF EXISTS t_multiply_i128_overflow;
CREATE TABLE t_multiply_i128_overflow (key Int128) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_i128_overflow SELECT number FROM numbers(10);
INSERT INTO t_multiply_i128_overflow VALUES (-56713727820156410577229101238628035243);
OPTIMIZE TABLE t_multiply_i128_overflow FINAL;

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_i128_overflow WHERE key * toInt128(3) > 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 9: UInt256 — pruning works
-- ============================================================

DROP TABLE IF EXISTS t_multiply_u256;
CREATE TABLE t_multiply_u256 (key UInt256) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_u256 SELECT number FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_u256 WHERE key * toUInt256(5) > 25) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 10: Int256 — overflow, all granules kept
-- ============================================================

DROP TABLE IF EXISTS t_multiply_i256_overflow;
CREATE TABLE t_multiply_i256_overflow (key Int256) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_i256_overflow SELECT number FROM numbers(10);
INSERT INTO t_multiply_i256_overflow VALUES (-19298681539552699237261830834781317975544997444273427339909597334652188273324);
OPTIMIZE TABLE t_multiply_i256_overflow FINAL;

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_i256_overflow WHERE key * toInt256(3) > 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 11: Constant on the left — `C * key`
-- ============================================================

DROP TABLE IF EXISTS t_multiply_left_const;
CREATE TABLE t_multiply_left_const (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_left_const SELECT number FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_left_const WHERE 2 * key > 10) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 12: Multiply by 1 — identity
-- ============================================================

DROP TABLE IF EXISTS t_multiply_one;
CREATE TABLE t_multiply_one (key Int64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_one SELECT number FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_one WHERE key * 1 > 5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 13: key * -1 with INT64_MIN — overflow detected in
-- the first granule (contains INT64_MIN); only that granule
-- is conservatively kept, rest pruned normally (10/11 kept)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_neg1_min;
CREATE TABLE t_multiply_neg1_min (key Int64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_neg1_min SELECT number FROM numbers(10);
INSERT INTO t_multiply_neg1_min VALUES (-9223372036854775808);
OPTIMIZE TABLE t_multiply_neg1_min FINAL;

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_neg1_min WHERE key * -1 < -1) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 14: key * -1 without overflow — decreasing monotonicity
-- ============================================================

DROP TABLE IF EXISTS t_multiply_neg1_ok;
CREATE TABLE t_multiply_neg1_ok (key Int64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_neg1_ok SELECT number FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_neg1_ok WHERE key * -1 < -5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 15: variable * variable — cannot use primary key
-- ============================================================

DROP TABLE IF EXISTS t_multiply_var_var;
CREATE TABLE t_multiply_var_var (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_var_var SELECT number, number + 1 FROM numbers(10);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_var_var WHERE key * val > 50) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- ============================================================
-- Part 16: Boundary — endpoint exactly at TYPE_MAX / c (no overflow)
-- ============================================================

DROP TABLE IF EXISTS t_multiply_boundary;
CREATE TABLE t_multiply_boundary (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_multiply_boundary SELECT number FROM numbers(10);
-- 6148914691236517205 * 3 = 18446744073709551615 = UInt64 max (no overflow).
INSERT INTO t_multiply_boundary VALUES (6148914691236517205);
OPTIMIZE TABLE t_multiply_boundary FINAL;

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multiply_boundary WHERE key * 3 > 30) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

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
DROP TABLE t_multiply_i128_overflow;
DROP TABLE t_multiply_u256;
DROP TABLE t_multiply_i256_overflow;
DROP TABLE t_multiply_left_const;
DROP TABLE t_multiply_one;
DROP TABLE t_multiply_neg1_min;
DROP TABLE t_multiply_neg1_ok;
DROP TABLE t_multiply_var_var;
DROP TABLE t_multiply_boundary;
