-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test basic aggregation functions
SELECT '=== Basic aggregation functions ===';

-- Create test table
DROP TABLE IF EXISTS test_decimal512_agg;
CREATE TABLE test_decimal512_agg (
    id UInt32,
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_agg VALUES 
(1, toDecimal512('123.456789', 6)),
(2, toDecimal512('234.567890', 6)),
(3, toDecimal512('345.678901', 6)),
(4, toDecimal512('456.789012', 6)),
(5, toDecimal512('567.890123', 6));

-- Test basic aggregation
SELECT 
    count(*) AS count,
    sum(value) AS sum_value,
    avg(value) AS avg_value,
    min(value) AS min_value,
    max(value) AS max_value
FROM test_decimal512_agg;

-- Test with empty table
DROP TABLE IF EXISTS test_decimal512_empty;
CREATE TABLE test_decimal512_empty (
    value Decimal512(6)
) ENGINE = Memory;

SELECT 
    count(*) AS count_empty,
    sum(value) AS sum_empty,
    avg(value) AS avg_empty,
    min(value) AS min_empty,
    max(value) AS max_empty
FROM test_decimal512_empty;

-- Test with single value
DROP TABLE IF EXISTS test_decimal512_single;
CREATE TABLE test_decimal512_single (
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_single VALUES (toDecimal512('999.999999', 6));

SELECT 
    count(*) AS count_single,
    sum(value) AS sum_single,
    avg(value) AS avg_single,
    min(value) AS min_single,
    max(value) AS max_single
FROM test_decimal512_single;

-- Test with negative values
DROP TABLE IF EXISTS test_decimal512_negative;
CREATE TABLE test_decimal512_negative (
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_negative VALUES 
(toDecimal512('-123.456789', 6)),
(toDecimal512('234.567890', 6)),
(toDecimal512('-345.678901', 6)),
(toDecimal512('456.789012', 6)),
(toDecimal512('-567.890123', 6));

SELECT 
    count(*) AS count_negative,
    sum(value) AS sum_negative,
    avg(value) AS avg_negative,
    min(value) AS min_negative,
    max(value) AS max_negative
FROM test_decimal512_negative;

-- Test with zero values
DROP TABLE IF EXISTS test_decimal512_zero;
CREATE TABLE test_decimal512_zero (
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_zero VALUES 
(toDecimal512('0.0', 6)),
(toDecimal512('123.456789', 6)),
(toDecimal512('0.0', 6)),
(toDecimal512('456.789012', 6)),
(toDecimal512('0.0', 6));

SELECT 
    count(*) AS count_zero,
    sum(value) AS sum_zero,
    avg(value) AS avg_zero,
    min(value) AS min_zero,
    max(value) AS max_zero
FROM test_decimal512_zero;

-- Clean up
DROP TABLE test_decimal512_agg;
DROP TABLE test_decimal512_empty;
DROP TABLE test_decimal512_single;
DROP TABLE test_decimal512_negative;
DROP TABLE test_decimal512_zero;

