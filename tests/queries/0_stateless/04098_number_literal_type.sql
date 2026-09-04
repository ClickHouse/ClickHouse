-- Test that numeric literals are parsed as NumberLiteral to preserve precision.
-- Before this fix, numbers > UInt64 max were parsed as Float64, silently losing precision.
-- Decimal-point literals were also parsed as Float64, losing precision for Decimal targets.

--
-- 1. Type inference for big integers (positive and negative)
--

SELECT toTypeName(100000000000000000000000);
SELECT toTypeName(-100000000000000000000000);

-- UInt64 max boundary: 18446744073709551615
SELECT toTypeName(18446744073709551615);   -- max UInt64, stays UInt64
SELECT toTypeName(18446744073709551616);   -- max UInt64 + 1, becomes UInt128

-- Int64 min boundary: -9223372036854775808
SELECT toTypeName(-9223372036854775808);   -- min Int64, stays Int64
SELECT toTypeName(-9223372036854775809);   -- min Int64 - 1, becomes Int128

-- UInt128 max boundary: 340282366920938463463374607431768211455
SELECT toTypeName(340282366920938463463374607431768211455);   -- UInt128 max, stays UInt128
SELECT toTypeName(340282366920938463463374607431768211456);   -- UInt128 max + 1, becomes UInt256

-- Int128 boundary (negative)
SELECT toTypeName(-170141183460469231731687303715884105729); -- Int128 min - 1, becomes Int256

-- UInt256 max boundary
SELECT toTypeName(115792089237316195423570985008687907853269984665640564039457584007913129639935);  -- UInt256 max
SELECT toTypeName(115792089237316195423570985008687907853269984665640564039457584007913129639936);  -- UInt256 max + 1, falls to Float64

-- Decimal-point literal default type (backward compat)
SELECT toTypeName(3.14);

--
-- 2. Precision preservation: big integer arithmetic
--

SELECT 100000000000000000000001 - 100000000000000000000000;
SELECT 100000000000000000000500 - 100000000000000000000000;
SELECT -100000000000000000000500 + 100000000000000000000000;

-- Boundary arithmetic
SELECT 18446744073709551616 - 18446744073709551615;

--
-- 3. BETWEEN with UInt128 column (original bug from #74312)
--

DROP TABLE IF EXISTS test_number_literal;
CREATE TABLE test_number_literal (action_id UInt128) ENGINE = MergeTree ORDER BY action_id;
INSERT INTO test_number_literal SELECT number + 100000000000000000000000 FROM numbers(1000);

SELECT count() FROM test_number_literal
WHERE action_id BETWEEN 100000000000000000000000 AND 100000000000000000000500;

SELECT count() FROM test_number_literal
WHERE action_id >= 100000000000000000000000 AND action_id <= 100000000000000000000500;

DROP TABLE test_number_literal;

--
-- 4. Value preservation and equality (positive and negative)
--

SELECT 100000000000000000000000;
SELECT -100000000000000000000000;

SELECT 100000000000000000000123 = 100000000000000000000123;
SELECT 100000000000000000000123 = 100000000000000000000124;

SELECT -100000000000000000000123 = -100000000000000000000123;
SELECT -100000000000000000000123 = -100000000000000000000124;

-- Boundary values preserved exactly
SELECT 340282366920938463463374607431768211455;   -- UInt128 max
SELECT 340282366920938463463374607431768211456;   -- UInt128 max + 1, now UInt256

--
-- 5. Big literals in various contexts
--

SELECT toTypeName([100000000000000000000000, 100000000000000000000001]);
SELECT 100000000000000000000000 + 1;
SELECT -100000000000000000000000 - 1;

--
-- 6. Comparison with Int128 column (positive and negative)
--

DROP TABLE IF EXISTS test_int128;
CREATE TABLE test_int128 (v Int128) ENGINE = MergeTree ORDER BY v;
INSERT INTO test_int128 VALUES (-100000000000000000000000), (100000000000000000000000);

SELECT count() FROM test_int128 WHERE v = -100000000000000000000000;
SELECT count() FROM test_int128 WHERE v = 100000000000000000000000;
SELECT count() FROM test_int128 WHERE v > -100000000000000000000001 AND v < 100000000000000000000001;

DROP TABLE test_int128;

--
-- 7. Decimal precision: deferred parsing avoids Float64 intermediate
--

DROP TABLE IF EXISTS test_decimal_literal;
CREATE TABLE test_decimal_literal (d Decimal128(18)) ENGINE = MergeTree ORDER BY d;
INSERT INTO test_decimal_literal VALUES ('1.123456789012345678');

-- Exact match: literal parsed directly as Decimal128 (not through Float64)
SELECT * FROM test_decimal_literal WHERE d = 1.123456789012345678;

-- Non-match: last digit differs (was false positive via Float64)
SELECT count() FROM test_decimal_literal WHERE d = 1.123456789012345679;

DROP TABLE test_decimal_literal;

--
-- 8. String target: CAST preserves original text
--

SELECT CAST(100000000000000000000000, 'String');
SELECT CAST(-100000000000000000000000, 'String');

--
-- 9. Numbers that overflow all integer types fall back to Float64
--

SELECT toTypeName(99999999999999999999999999999999999999999999999999999999999999999999999999999999);

--
-- 10. Exact minimum signed integer boundaries (roundtrip verification must handle min values)
--

-- Int128 min = -170141183460469231731687303715884105728
SELECT toTypeName(-170141183460469231731687303715884105728);
SELECT -170141183460469231731687303715884105728;

-- Int256 min = -57896044618658097711785492504343953926634992332820282019728792003956564819968
SELECT toTypeName(-57896044618658097711785492504343953926634992332820282019728792003956564819968);
SELECT -57896044618658097711785492504343953926634992332820282019728792003956564819968;

--
-- 11. Scientific notation literals with Decimal comparison (must not corrupt scale)
--

DROP TABLE IF EXISTS test_decimal_sci;
CREATE TABLE test_decimal_sci (d Decimal64(4)) ENGINE = MergeTree ORDER BY d;
INSERT INTO test_decimal_sci VALUES (0.01);

-- 1e-2 = 0.01 — scientific notation should resolve as Float64, not Decimal with wrong scale
SELECT count() FROM test_decimal_sci WHERE d = 1e-2;

-- 1.23e-2 = 0.0123 — same, no broken Decimal scale
SELECT count() FROM test_decimal_sci WHERE d = 1.23e-2;

-- Plain decimal: exact match via Decimal
SELECT count() FROM test_decimal_sci WHERE d = 0.01;

DROP TABLE test_decimal_sci;
