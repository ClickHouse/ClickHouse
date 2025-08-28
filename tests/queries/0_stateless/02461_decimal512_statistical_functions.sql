-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test statistical aggregation functions
SELECT '=== Statistical aggregation functions ===';

-- Create test table
DROP TABLE IF EXISTS test_decimal512_stats;
CREATE TABLE test_decimal512_stats (
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_stats VALUES 
(toDecimal512('100.0', 1)),
(toDecimal512('200.0', 1)),
(toDecimal512('300.0', 1)),
(toDecimal512('400.0', 1)),
(toDecimal512('500.0', 1));

-- Test statistical functions
SELECT 
    stddevPop(value) AS stddev_pop,
    varPop(value) AS var_pop,
    quantile(0.5)(value) AS median,
    quantile(0.25)(value) AS q25,
    quantile(0.75)(value) AS q75
FROM test_decimal512_stats;

-- Test with different quantiles
SELECT 
    quantile(0.1)(value) AS q10,
    quantile(0.2)(value) AS q20,
    quantile(0.3)(value) AS q30,
    quantile(0.4)(value) AS q40,
    quantile(0.6)(value) AS q60,
    quantile(0.7)(value) AS q70,
    quantile(0.8)(value) AS q80,
    quantile(0.9)(value) AS q90
FROM test_decimal512_stats;

-- Test with negative values
DROP TABLE IF EXISTS test_decimal512_stats_negative;
CREATE TABLE test_decimal512_stats_negative (
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_stats_negative VALUES 
(toDecimal512('-100.0', 1)),
(toDecimal512('-200.0', 1)),
(toDecimal512('0.0', 1)),
(toDecimal512('200.0', 1)),
(toDecimal512('400.0', 1));

SELECT 
    stddevPop(value) AS stddev_pop_negative,
    varPop(value) AS var_pop_negative,
    quantile(0.5)(value) AS median_negative,
    quantile(0.25)(value) AS q25_negative,
    quantile(0.75)(value) AS q75_negative
FROM test_decimal512_stats_negative;

-- Test with single value
DROP TABLE IF EXISTS test_decimal512_stats_single;
CREATE TABLE test_decimal512_stats_single (
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_stats_single VALUES (toDecimal512('999.999999', 6));

SELECT 
    stddevPop(value) AS stddev_pop_single,
    varPop(value) AS var_pop_single,
    quantile(0.5)(value) AS median_single,
    quantile(0.25)(value) AS q25_single,
    quantile(0.75)(value) AS q75_single
FROM test_decimal512_stats_single;

-- Test with identical values
DROP TABLE IF EXISTS test_decimal512_stats_identical;
CREATE TABLE test_decimal512_stats_identical (
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_stats_identical VALUES 
(toDecimal512('123.456789', 6)),
(toDecimal512('123.456789', 6)),
(toDecimal512('123.456789', 6)),
(toDecimal512('123.456789', 6)),
(toDecimal512('123.456789', 6));

SELECT 
    stddevPop(value) AS stddev_pop_identical,
    varPop(value) AS var_pop_identical,
    quantile(0.5)(value) AS median_identical,
    quantile(0.25)(value) AS q25_identical,
    quantile(0.75)(value) AS q75_identical
FROM test_decimal512_stats_identical;

-- Test with very large numbers
DROP TABLE IF EXISTS test_decimal512_stats_large;
CREATE TABLE test_decimal512_stats_large (
    value Decimal512(6)
) ENGINE = Memory;

INSERT INTO test_decimal512_stats_large VALUES 
(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.123456', 6)),
(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999998.123456', 6)),
(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999997.123456', 6));

SELECT 
    stddevPop(value) AS stddev_pop_large,
    varPop(value) AS var_pop_large,
    quantile(0.5)(value) AS median_large,
    quantile(0.25)(value) AS q25_large,
    quantile(0.75)(value) AS q75_large
FROM test_decimal512_stats_large;

-- Clean up
DROP TABLE test_decimal512_stats;
DROP TABLE test_decimal512_stats_negative;
DROP TABLE test_decimal512_stats_single;
DROP TABLE test_decimal512_stats_identical;
DROP TABLE test_decimal512_stats_large;

