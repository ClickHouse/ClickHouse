-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: impacts index scoring

-- Create test table with 20 columns
CREATE TABLE test_index_advisor (
    id UInt32,
    col1 String,
    col2 Int32,
    col3 Float64,
    col4 Date,
    col5 DateTime,
    col6 UInt64,
    col7 Int64,
    col8 Decimal32(2),
    col9 String,
    col10 Int32,
    col11 Float64,
    col12 Date,
    col13 DateTime,
    col14 UInt64,
    col15 Int64,
    col16 Decimal32(2),
    col17 String,
    col18 Int32,
    col19 Float64,
    col20 Date
) ENGINE = MergeTree() ORDER BY id;

-- Insert 20000 rows of test data
INSERT INTO test_index_advisor
SELECT 
    number as id,
    concat('str_', toString(number)) as col1,
    number as col2,
    number * 1.1 as col3,
    toDate('2024-01-01') + number as col4,
    toDateTime('2024-01-01 00:00:00') + number as col5,
    number as col6,
    number as col7,
    toDecimal32(number * 1.1, 2) as col8,
    concat('str_', toString(number + 1000)) as col9,
    number + 1000 as col10,
    (number + 1000) * 1.1 as col11,
    toDate('2024-01-01') + number + 1000 as col12,
    toDateTime('2024-01-01 00:00:00') + number + 1000 as col13,
    number + 1000 as col14,
    number + 1000 as col15,
    toDecimal32((number + 1000) * 1.1, 2) as col16,
    concat('str_', toString(number + 2000)) as col17,
    2000 + (number % 100) as col18,  -- Only 100 unique values from 2000 to 2099
    (number + 2000) * 1.1 as col19,
    toDate('2024-01-01') + number + 2000 as col20
FROM numbers(20000);

-- Test index advice for each column with equality condition
ADVISE INDEX (
    SELECT * FROM test_index_advisor WHERE col1 = 'str_100',
    SELECT * FROM test_index_advisor WHERE col2 = 100,
    SELECT * FROM test_index_advisor WHERE col3 = 110.0,
    SELECT * FROM test_index_advisor WHERE col4 = '2024-01-01',
    SELECT * FROM test_index_advisor WHERE col5 = '2024-01-01 00:00:00',
    SELECT * FROM test_index_advisor WHERE col6 = 100,
    SELECT * FROM test_index_advisor WHERE col7 = 100,
    SELECT * FROM test_index_advisor WHERE col8 = toDecimal32(110, 2),
    SELECT * FROM test_index_advisor WHERE col9 = 'str_1100',
    SELECT * FROM test_index_advisor WHERE col10 = 1100,
    SELECT * FROM test_index_advisor WHERE col11 = 1210.0,
    SELECT * FROM test_index_advisor WHERE col12 = '2024-01-01',
    SELECT * FROM test_index_advisor WHERE col13 = '2024-01-01 00:00:00',
    SELECT * FROM test_index_advisor WHERE col14 = 1100,
    SELECT * FROM test_index_advisor WHERE col15 = 1100,
    SELECT * FROM test_index_advisor WHERE col16 = toDecimal32(1210, 2),
    SELECT * FROM test_index_advisor WHERE col17 = 'str_2100',
    SELECT * FROM test_index_advisor WHERE col18 = 2099,
    SELECT * FROM test_index_advisor WHERE col19 = 2310.0,
    SELECT * FROM test_index_advisor WHERE col20 = '2024-01-01'
);

-- Cleanup
DROP TABLE IF EXISTS test_index_advisor; 