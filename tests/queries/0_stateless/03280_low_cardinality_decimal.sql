SET allow_suspicious_low_cardinality_types = 0;
DROP TABLE IF EXISTS x;
CREATE TABLE x (x LowCardinality(Decimal32(1))) ENGINE = MergeTree ORDER BY x; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
CREATE TABLE x (x LowCardinality(Decimal64(1))) ENGINE = MergeTree ORDER BY x; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
CREATE TABLE x (x LowCardinality(Decimal128(1))) ENGINE = MergeTree ORDER BY x;
DROP TABLE x;
CREATE TABLE x (x LowCardinality(Decimal256(1))) ENGINE = MergeTree ORDER BY x;
DROP TABLE x;

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE x (x LowCardinality(Decimal32(1))) ENGINE = MergeTree ORDER BY x;
DROP TABLE x;
CREATE TABLE x (x LowCardinality(Decimal64(1))) ENGINE = MergeTree ORDER BY x;
DROP TABLE x;
CREATE TABLE x (x LowCardinality(Decimal128(1))) ENGINE = MergeTree ORDER BY x;
DROP TABLE x;
CREATE TABLE x (x LowCardinality(Decimal256(1))) ENGINE = MergeTree ORDER BY x;
DROP TABLE x;

-- Test for low cardinality decimal
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS test_low_cardinality_decimal;
-- Create a table with LowCardinalitLowCardinalityy and Decimal columns
CREATE TABLE test_low_cardinality_decimal (
    id UInt32,
    value LowCardinality(Decimal(10, 2))
) ENGINE = Memory;

-- Insert some values into the table
INSERT INTO test_low_cardinality_decimal VALUES
(1, 123.45),
(2, 678.90),
(3, 123.45),
(4, 678.90),
(5, 123.45);

-- Select all values to ensure they are inserted correctly
SELECT * FROM test_low_cardinality_decimal ORDER BY id;

-- Test aggregation on LowCardinality Decimal column
SELECT value, COUNT(*)
FROM test_low_cardinality_decimal
GROUP BY value ORDER BY value;

-- Test filtering on LowCardinality Decimal column
SELECT *
FROM test_low_cardinality_decimal
WHERE value = 123.45 ORDER BY id;

-- Drop the table after tests
DROP TABLE test_low_cardinality_decimal;

-- Test for low cardinality DateTime64
DROP TABLE IF EXISTS test_low_cardinality_datetime64;
-- Create a table with LowCardinality and DateTime64 columns
CREATE TABLE test_low_cardinality_datetime64 (
    id UInt32,
    event_time LowCardinality(DateTime64(3))
) ENGINE = Memory;

-- Insert some values into the table
INSERT INTO test_low_cardinality_datetime64 VALUES
(1, '2023-01-01 12:00:00.123'),
(2, '2023-01-02 12:00:00.456'),
(3, '2023-01-01 12:00:00.123'),
(4, '2023-01-02 12:00:00.456'),
(5, '2023-01-01 12:00:00.123');

-- Select all values to ensure they are inserted correctly
SELECT * FROM test_low_cardinality_datetime64 ORDER BY id;

-- Test aggregation on LowCardinality DateTime64 column
SELECT event_time, COUNT(*)
FROM test_low_cardinality_datetime64
GROUP BY event_time ORDER BY event_time;

-- Test filtering on LowCardinality DateTime64 column
SELECT *
FROM test_low_cardinality_datetime64
WHERE event_time = '2023-01-01 12:00:00.123' ORDER BY id;

-- Drop the table after tests
DROP TABLE test_low_cardinality_datetime64;
-- Test for LowCardinality Decimal32

DROP TABLE IF EXISTS test_low_cardinality_decimal32;
-- Create a table with LowCardinality and Decimal32 columns
CREATE TABLE test_low_cardinality_decimal32 (
    id UInt32,
    value LowCardinality(Decimal32(2))
) ENGINE = Memory;

-- Insert some values into the table
INSERT INTO test_low_cardinality_decimal32 VALUES
(1, 123.45),
(2, 678.90),
(3, 123.45),
(4, 678.90),
(5, 123.45);

-- Select all values to ensure they are inserted correctly
SELECT * FROM test_low_cardinality_decimal32 ORDER BY id;

-- Test aggregation on LowCardinality Decimal32 column
SELECT value, COUNT(*)
FROM test_low_cardinality_decimal32
GROUP BY value ORDER BY value;

-- Test filtering on LowCardinality Decimal32 column
SELECT *
FROM test_low_cardinality_decimal32
WHERE value = 123.45 ORDER BY id;

-- Drop the table after tests
DROP TABLE test_low_cardinality_decimal32;

-- Test for LowCardinality Decimal64
DROP TABLE IF EXISTS test_low_cardinality_decimal64;
-- Create a table with LowCardinality and Decimal64 columns
CREATE TABLE test_low_cardinality_decimal64 (
    id UInt32,
    value LowCardinality(Decimal64(2))
) ENGINE = Memory;

-- Insert some values into the table
INSERT INTO test_low_cardinality_decimal64 VALUES
(1, 123.45),
(2, 678.90),
(3, 123.45),
(4, 678.90),
(5, 123.45);

-- Select all values to ensure they are inserted correctly
SELECT * FROM test_low_cardinality_decimal64 ORDER BY id;

-- Test aggregation on LowCardinality Decimal64 column
SELECT value, COUNT(*)
FROM test_low_cardinality_decimal64
GROUP BY value ORDER BY value;

-- Test filtering on LowCardinality Decimal64 column
SELECT *
FROM test_low_cardinality_decimal64
WHERE value = 123.45 ORDER BY id;

-- Drop the table after tests
DROP TABLE test_low_cardinality_decimal64;

-- Test for LowCardinality Decimal128
DROP TABLE IF EXISTS test_low_cardinality_decimal128;
-- Create a table with LowCardinality and Decimal128 columns
CREATE TABLE test_low_cardinality_decimal128 (
    id UInt32,
    value LowCardinality(Decimal128(2))
) ENGINE = Memory;

-- Insert some values into the table
INSERT INTO test_low_cardinality_decimal128 VALUES
(1, 123.45),
(2, 678.90),
(3, 123.45),
(4, 678.90),
(5, 123.45);

-- Select all values to ensure they are inserted correctly
SELECT * FROM test_low_cardinality_decimal128 ORDER BY id;

-- Test aggregation on LowCardinality Decimal128 column
SELECT value, COUNT(*)
FROM test_low_cardinality_decimal128
GROUP BY value ORDER BY value;

-- Test filtering on LowCardinality Decimal128 column
SELECT *
FROM test_low_cardinality_decimal128
WHERE value = 123.45 ORDER BY id;

-- Drop the table after tests
DROP TABLE test_low_cardinality_decimal128;

-- Test for LowCardinality Decimal256
DROP TABLE IF EXISTS test_low_cardinality_decimal256;
-- Create a table with LowCardinality and Decimal256 columns
CREATE TABLE test_low_cardinality_decimal256 (
    id UInt32,
    value LowCardinality(Decimal256(2))
) ENGINE = Memory;

-- Insert some values into the table
INSERT INTO test_low_cardinality_decimal256 VALUES
(1, 123.45),
(2, 678.90),
(3, 123.45),
(4, 678.90),
(5, 123.45);

-- Select all values to ensure they are inserted correctly
SELECT * FROM test_low_cardinality_decimal256 ORDER BY id;

-- Test aggregation on LowCardinality Decimal256 column
SELECT value, COUNT(*)
FROM test_low_cardinality_decimal256
GROUP BY value ORDER BY value;

-- Test filtering on LowCardinality Decimal256 column
SELECT *
FROM test_low_cardinality_decimal256
WHERE value = 123.45 ORDER BY id;

-- Drop the table after tests
DROP TABLE test_low_cardinality_decimal256;
