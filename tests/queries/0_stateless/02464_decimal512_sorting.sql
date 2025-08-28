-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test sorting functionality
SELECT '=== Sorting functionality ===';

-- Create test table
DROP TABLE IF EXISTS test_decimal512_sort;
CREATE TABLE test_decimal512_sort (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_sort VALUES 
(1, toDecimal512('123.456789', 6)),
(2, toDecimal512('234.567890', 6)),
(3, toDecimal512('345.678901', 6)),
(4, toDecimal512('456.789012', 6)),
(5, toDecimal512('567.890123', 6));

-- Test ascending sort
SELECT '=== Ascending sort ===';
SELECT * FROM test_decimal512_sort ORDER BY value;

-- Test descending sort
SELECT '=== Descending sort ===';
SELECT * FROM test_decimal512_sort ORDER BY value DESC;

-- Test multi-column sort
SELECT '=== Multi-column sort ===';
SELECT * FROM test_decimal512_sort ORDER BY id, value;
SELECT * FROM test_decimal512_sort ORDER BY id DESC, value DESC;

-- Test with negative values
DROP TABLE IF EXISTS test_decimal512_sort_negative;
CREATE TABLE test_decimal512_sort_negative (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_sort_negative VALUES 
(1, toDecimal512('-123.456789', 6)),
(2, toDecimal512('234.567890', 6)),
(3, toDecimal512('-345.678901', 6)),
(4, toDecimal512('456.789012', 6)),
(5, toDecimal512('-567.890123', 6));

SELECT '=== Sort with negative values ===';
SELECT * FROM test_decimal512_sort_negative ORDER BY value;
SELECT * FROM test_decimal512_sort_negative ORDER BY value DESC;

-- Test with zero values
DROP TABLE IF EXISTS test_decimal512_sort_zero;
CREATE TABLE test_decimal512_sort_zero (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_sort_zero VALUES 
(1, toDecimal512('0.0', 6)),
(2, toDecimal512('123.456789', 6)),
(3, toDecimal512('0.0', 6)),
(4, toDecimal512('456.789012', 6)),
(5, toDecimal512('0.0', 6));

SELECT '=== Sort with zero values ===';
SELECT * FROM test_decimal512_sort_zero ORDER BY value;
SELECT * FROM test_decimal512_sort_zero ORDER BY value DESC;

-- Test with identical values
DROP TABLE IF EXISTS test_decimal512_sort_identical;
CREATE TABLE test_decimal512_sort_identical (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_sort_identical VALUES 
(1, toDecimal512('123.456789', 6)),
(2, toDecimal512('123.456789', 6)),
(3, toDecimal512('123.456789', 6)),
(4, toDecimal512('123.456789', 6)),
(5, toDecimal512('123.456789', 6));

SELECT '=== Sort with identical values ===';
SELECT * FROM test_decimal512_sort_identical ORDER BY value;
SELECT * FROM test_decimal512_sort_identical ORDER BY value DESC;

-- Test with very large numbers
DROP TABLE IF EXISTS test_decimal512_sort_large;
CREATE TABLE test_decimal512_sort_large (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_sort_large VALUES 
(1, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.123456', 6)),
(2, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999998.123456', 6)),
(3, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999997.123456', 6));

SELECT '=== Sort with very large numbers ===';
SELECT * FROM test_decimal512_sort_large ORDER BY value;
SELECT * FROM test_decimal512_sort_large ORDER BY value DESC;

-- Test LIMIT with ORDER BY
SELECT '=== LIMIT with ORDER BY ===';
SELECT * FROM test_decimal512_sort ORDER BY value LIMIT 3;
SELECT * FROM test_decimal512_sort ORDER BY value DESC LIMIT 3;

-- Clean up
DROP TABLE test_decimal512_sort;
DROP TABLE test_decimal512_sort_negative;
DROP TABLE test_decimal512_sort_zero;
DROP TABLE test_decimal512_sort_identical;
DROP TABLE test_decimal512_sort_large;

