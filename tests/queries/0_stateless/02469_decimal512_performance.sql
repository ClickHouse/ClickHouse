-- Tags: no_asan, no_msan, no_tsan, no_ubsan, no_debug

-- Test performance with large datasets
SELECT '=== Performance tests ===';

-- Create large test table
DROP TABLE IF EXISTS test_decimal512_large;
CREATE TABLE test_decimal512_large (
    id UInt64,
    value Decimal512(6)
) ENGINE = Memory;

-- Insert large amount of data
INSERT INTO test_decimal512_large 
SELECT number, toDecimal512(toString(number * 1.234567), 6) 
FROM numbers(10000);

-- Test performance queries
SELECT '=== Performance queries ===';
SELECT count(*), sum(value), avg(value) FROM test_decimal512_large;

-- Test sorting performance
SELECT '=== Sorting performance ===';
SELECT * FROM test_decimal512_large ORDER BY value LIMIT 10;

-- Test grouping performance
SELECT '=== Grouping performance ===';
SELECT value % toDecimal512('100.0', 1) AS group_key, count(*) FROM test_decimal512_large GROUP BY group_key ORDER BY group_key LIMIT 10;

-- Clean up
DROP TABLE test_decimal512_large;

