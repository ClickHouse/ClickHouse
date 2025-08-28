-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test grouping functionality
SELECT '=== Grouping functionality ===';

-- Create test table
DROP TABLE IF EXISTS test_decimal512_group;
CREATE TABLE test_decimal512_group (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_group VALUES 
(1, toDecimal512('123.456789', 6)),
(2, toDecimal512('234.567890', 6)),
(3, toDecimal512('123.456789', 6)),
(4, toDecimal512('456.789012', 6)),
(5, toDecimal512('234.567890', 6));

-- Test basic grouping
SELECT '=== Basic grouping ===';
SELECT 
    value,
    count(*) AS count,
    sum(value) AS sum_value
FROM test_decimal512_group 
GROUP BY value
ORDER BY value;

-- Test grouping with aggregation
SELECT '=== Grouping with aggregation ===';
SELECT 
    value,
    count(*) AS count,
    sum(value) AS sum_value,
    avg(value) AS avg_value,
    min(value) AS min_value,
    max(value) AS max_value
FROM test_decimal512_group 
GROUP BY value
ORDER BY value;

-- Test grouping with HAVING
SELECT '=== Grouping with HAVING ===';
SELECT 
    value,
    count(*) AS count
FROM test_decimal512_group 
GROUP BY value
HAVING count(*) > 1
ORDER BY value;

-- Test with negative values
DROP TABLE IF EXISTS test_decimal512_group_negative;
CREATE TABLE test_decimal512_group_negative (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_group_negative VALUES 
(1, toDecimal512('-123.456789', 6)),
(2, toDecimal512('234.567890', 6)),
(3, toDecimal512('-123.456789', 6)),
(4, toDecimal512('-456.789012', 6)),
(5, toDecimal512('234.567890', 6));

SELECT '=== Grouping with negative values ===';
SELECT 
    value,
    count(*) AS count,
    sum(value) AS sum_value
FROM test_decimal512_group_negative 
GROUP BY value
ORDER BY value;

-- Test with zero values
DROP TABLE IF EXISTS test_decimal512_group_zero;
CREATE TABLE test_decimal512_group_zero (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_group_zero VALUES 
(1, toDecimal512('0.0', 6)),
(2, toDecimal512('123.456789', 6)),
(3, toDecimal512('0.0', 6)),
(4, toDecimal512('456.789012', 6)),
(5, toDecimal512('0.0', 6));

SELECT '=== Grouping with zero values ===';
SELECT 
    value,
    count(*) AS count,
    sum(value) AS sum_value
FROM test_decimal512_group_zero 
GROUP BY value
ORDER BY value;

-- Test with identical values
DROP TABLE IF EXISTS test_decimal512_group_identical;
CREATE TABLE test_decimal512_group_identical (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_group_identical VALUES 
(1, toDecimal512('123.456789', 6)),
(2, toDecimal512('123.456789', 6)),
(3, toDecimal512('123.456789', 6)),
(4, toDecimal512('123.456789', 6)),
(5, toDecimal512('123.456789', 6));

SELECT '=== Grouping with identical values ===';
SELECT 
    value,
    count(*) AS count,
    sum(value) AS sum_value
FROM test_decimal512_group_identical 
GROUP BY value
ORDER BY value;

-- Test DISTINCT
SELECT '=== DISTINCT ===';
SELECT DISTINCT value FROM test_decimal512_group ORDER BY value;
SELECT DISTINCT value FROM test_decimal512_group_negative ORDER BY value;
SELECT DISTINCT value FROM test_decimal512_group_zero ORDER BY value;

-- Test grouping with different scales
DROP TABLE IF EXISTS test_decimal512_group_scales;
CREATE TABLE test_decimal512_group_scales (
    id UInt32,
    value1 Decimal512(3),
    value2 Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_group_scales VALUES 
(1, toDecimal512('123.456', 3), toDecimal512('123.456789', 6)),
(2, toDecimal512('123.456', 3), toDecimal512('234.567890', 6)),
(3, toDecimal512('234.567', 3), toDecimal512('123.456789', 6)),
(4, toDecimal512('234.567', 3), toDecimal512('234.567890', 6));

SELECT '=== Grouping with different scales ===';
SELECT 
    value1,
    value2,
    count(*) AS count
FROM test_decimal512_group_scales 
GROUP BY value1, value2
ORDER BY value1, value2;

-- Clean up
DROP TABLE test_decimal512_group;
DROP TABLE test_decimal512_group_negative;
DROP TABLE test_decimal512_group_zero;
DROP TABLE test_decimal512_group_identical;
DROP TABLE test_decimal512_group_scales;

