-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- This test file provides extended validation for the Decimal512 type,
-- complementing the initial validation in 02452_decimal512_validation.sql.
-- It covers boundary values, a wider range of arithmetic operations,
-- comparisons, aggregate functions, and other standard functions.

SELECT '--- Boundary and Edge Case Validation ---';

DROP TABLE IF EXISTS decimal512_extended_test;
CREATE TABLE decimal512_extended_test (
    d_max_prec_pos Decimal(153, 50),
    d_max_prec_neg Decimal(153, 50),
    d_max_scale Decimal(153, 153),
    d_zero_scale Decimal(153, 0)
) ENGINE = Memory;

-- Test with max precision (153 digits)
-- Value is 103 digits before decimal point, 50 after.
INSERT INTO decimal512_extended_test (d_max_prec_pos) VALUES ('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123.12345678901234567890123456789012345678901234567890');
-- Test with negative value
INSERT INTO decimal512_extended_test (d_max_prec_neg) VALUES ('-1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123.12345678901234567890123456789012345678901234567890');
-- Test with max scale (value is < 1)
INSERT INTO decimal512_extended_test (d_max_scale) VALUES ('0.12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123');
-- Test with zero scale (integer)
INSERT INTO decimal512_extended_test (d_zero_scale) VALUES ('12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123');

SELECT toTypeName(d_max_prec_pos), toTypeName(d_max_scale), toTypeName(d_zero_scale) FROM decimal512_extended_test LIMIT 1;
SELECT d_max_prec_pos, d_max_prec_neg, d_max_scale, d_zero_scale FROM decimal512_extended_test WHERE d_max_prec_pos IS NOT NULL;

DROP TABLE decimal512_extended_test;

SELECT '--- Arithmetic Operations ---';

DROP TABLE IF EXISTS decimal_arith_test;
CREATE TABLE decimal_arith_test (
    a Decimal(100, 50),
    b Decimal(100, 50)
) ENGINE=Memory;

INSERT INTO decimal_arith_test VALUES ('1000.123456789', '-2000.987654321');

-- Subtraction
SELECT a - b FROM decimal_arith_test;
-- Division
SELECT b / a FROM decimal_arith_test;
SELECT a / 2 FROM decimal_arith_test;
-- Division by zero
SELECT a / 0 FROM decimal_arith_test; -- { serverError DIVISION_BY_ZERO }
-- Unary minus
SELECT -a FROM decimal_arith_test;
-- Multiplication with different scales
SELECT CAST(a AS Decimal(100, 20)) * CAST(b AS Decimal(100, 30)) FROM decimal_arith_test;

DROP TABLE decimal_arith_test;

SELECT '--- Comparison and Ordering ---';

DROP TABLE IF EXISTS decimal_cmp_test;
CREATE TABLE decimal_cmp_test (
    id UInt64,
    d Decimal(100, 2)
) ENGINE=Memory;

INSERT INTO decimal_cmp_test VALUES (1, '12345.67'), (2, '-98765.43'), (3, '12345.67'), (4, '0.00'), (5, '12345.68');

-- All comparison operators
SELECT id, d FROM decimal_cmp_test WHERE d = toDecimal512('12345.67', 2) ORDER BY id;
SELECT id, d FROM decimal_cmp_test WHERE d > toDecimal512('0.00', 2) ORDER BY id;
SELECT id, d FROM decimal_cmp_test WHERE d < toDecimal512('0.00', 2) ORDER BY id;

-- ORDER BY
SELECT d FROM decimal_cmp_test ORDER BY d ASC;
SELECT d FROM decimal_cmp_test ORDER BY d DESC;

-- GROUP BY and DISTINCT
SELECT count(), d FROM decimal_cmp_test GROUP BY d ORDER BY d;
SELECT DISTINCT d FROM decimal_cmp_test ORDER BY d;

DROP TABLE decimal_cmp_test;

SELECT '--- Aggregate and Other Functions ---';

DROP TABLE IF EXISTS decimal_func_test;
CREATE TABLE decimal_func_test (
    d Decimal(100, 2)
) ENGINE=Memory;

INSERT INTO decimal_func_test VALUES ('100.01'), ('200.02'), ('-50.00'), ('100.01');

-- Aggregate functions
SELECT SUM(d), AVG(d), MIN(d), MAX(d), COUNT(d) FROM decimal_func_test;
-- Test potential SUM overflow (this will not overflow 512, but good practice)
-- Let's create a table where sum could overflow smaller decimal types
DROP TABLE IF EXISTS decimal_sum_overflow;
CREATE TABLE decimal_sum_overflow (d Decimal(80, 2));
INSERT INTO decimal_sum_overflow SELECT toDecimal256('1e75', 2) FROM numbers(10);
SELECT SUM(d) FROM decimal_sum_overflow; -- This should produce a Decimal512
SELECT toTypeName(SUM(d)) FROM decimal_sum_overflow;
DROP TABLE decimal_sum_overflow;


-- Mathematical functions
SELECT abs(d), round(d), floor(d), ceil(d), truncate(d) FROM decimal_func_test LIMIT 1;

-- Casting
SELECT CAST(d AS Float64), CAST(d as String) FROM decimal_func_test LIMIT 1;
SELECT CAST('1234567890123456789012345678901234567890.12345' AS Decimal(100, 20));

DROP TABLE decimal_func_test;
