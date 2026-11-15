-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- This test file provides extended validation for the Decimal512 type,
-- complementing the initial validation in 02452_decimal512_validation.sql.
-- It covers boundary values, a wider range of arithmetic operations,
-- comparisons, aggregate functions, and other standard functions.

SELECT '--- Boundary and Edge Case Validation ---';

DROP TABLE IF EXISTS decimal512_extended_test;
CREATE TABLE decimal512_extended_test (
    d_max_prec_pos Decimal(154, 50),
    d_max_prec_neg Decimal(154, 50),
    d_max_scale Decimal(154, 154),
    d_zero_scale Decimal(154, 0)
) ENGINE = Memory;

-- Test with max precision (154 digits)
-- Value is 103 digits before decimal point, 50 after.
INSERT INTO decimal512_extended_test (d_max_prec_pos) VALUES ('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123.12345678901234567890123456789012345678901234567890');
-- Test with negative value
INSERT INTO decimal512_extended_test (d_max_prec_neg) VALUES ('-1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123.12345678901234567890123456789012345678901234567890');
-- Test with max scale (value is < 1)
INSERT INTO decimal512_extended_test (d_max_scale) VALUES ('0.12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123');
-- Test with zero scale (integer)
INSERT INTO decimal512_extended_test (d_zero_scale) VALUES ('12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123');

SELECT toTypeName(d_max_prec_pos), toTypeName(d_max_scale), toTypeName(d_zero_scale) FROM decimal512_extended_test LIMIT 1;
SELECT * FROM (
    SELECT d_max_prec_pos,
           CAST('0' AS Decimal(154, 50)),
           CAST('0' AS Decimal(154, 154)),
           CAST('0' AS Decimal(154, 0))
    FROM decimal512_extended_test WHERE d_max_prec_pos != CAST('0' AS Decimal(154, 50))
    UNION ALL
    SELECT CAST('0' AS Decimal(154, 50)),
           d_max_prec_neg,
           CAST('0' AS Decimal(154, 154)),
           CAST('0' AS Decimal(154, 0))
    FROM decimal512_extended_test WHERE d_max_prec_neg != CAST('0' AS Decimal(154, 50))
    UNION ALL
    SELECT CAST('0' AS Decimal(154, 50)),
           CAST('0' AS Decimal(154, 50)),
           d_max_scale,
           CAST('0' AS Decimal(154, 0))
    FROM decimal512_extended_test WHERE d_max_scale != CAST('0' AS Decimal(154, 154))
    UNION ALL
    SELECT CAST('0' AS Decimal(154, 50)),
           CAST('0' AS Decimal(154, 50)),
           CAST('0' AS Decimal(154, 154)),
           d_zero_scale
    FROM decimal512_extended_test WHERE d_zero_scale != CAST('0' AS Decimal(154, 0))
) ORDER BY 1, 2, 3, 4;

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
SELECT a / 0 FROM decimal_arith_test; -- { serverError ILLEGAL_DIVISION }
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


-- Mathematical functions
SELECT abs(d), round(d), floor(d), ceil(d), truncate(d) FROM decimal_func_test LIMIT 1;

-- Casting
SELECT CAST(d AS Float64), CAST(d as String) FROM decimal_func_test LIMIT 1;
SELECT CAST('1234567890123456789012345678901234567890.12345' AS Decimal(100, 20));

DROP TABLE decimal_func_test;
