SET allow_experimental_data_skipping_indices = 1;

DROP TABLE IF EXISTS test.single_column_bloom_filter;

CREATE TABLE test.single_column_bloom_filter (u64 UInt64, i32 Int32, i64 UInt64, INDEX idx (i32) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 6;

INSERT INTO test.single_column_bloom_filter SELECT number AS u64, number AS i32, number AS i64 FROM system.numbers LIMIT 100;

SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i32) = (1, 2) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i64) = (1, 1) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i64, (i64, i32)) = (1, (1, 1)) SETTINGS max_rows_to_read = 6;

SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 IN (1, 2) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i32) IN ((1, 2), (2, 3)) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i64) IN ((1, 1), (2, 2)) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i64, (i64, i32)) IN ((1, (1, 1)), (2, (2, 2))) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 IN (SELECT arrayJoin([toInt32(1), toInt32(2)])) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i32) IN (SELECT arrayJoin([(toInt32(1), toInt32(2)), (toInt32(2), toInt32(3))])) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i64) IN (SELECT arrayJoin([(toInt32(1), toUInt64(1)), (toInt32(2), toUInt64(2))])) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i64, (i64, i32)) IN (SELECT arrayJoin([(toUInt64(1), (toUInt64(1), toInt32(1))), (toUInt64(2), (toUInt64(2), toInt32(2)))])) SETTINGS max_rows_to_read = 6;
WITH (1, 2) AS liter_prepared_set SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 IN liter_prepared_set SETTINGS max_rows_to_read = 6;
WITH ((1, 2), (2, 3)) AS liter_prepared_set SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i32) IN liter_prepared_set SETTINGS max_rows_to_read = 6;
WITH ((1, 1), (2, 2)) AS liter_prepared_set SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i64) IN liter_prepared_set SETTINGS max_rows_to_read = 6;
WITH ((1, (1, 1)), (2, (2, 2))) AS liter_prepared_set SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i64, (i64, i32)) IN liter_prepared_set SETTINGS max_rows_to_read = 6;

DROP TABLE IF EXISTS test.single_column_bloom_filter;


DROP TABLE IF EXISTS test.bloom_filter_types_test;

CREATE TABLE test.bloom_filter_types_test (order_key   UInt64, i8 Int8, i16 Int16, i32 Int32, i64 Int64, u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64, f32 Float32, f64 Float64, date Date, date_time DateTime('Europe/Moscow'), str String, fixed_string FixedString(5), INDEX idx (i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, date, date_time, str, fixed_string) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY order_key SETTINGS index_granularity = 6;
INSERT INTO test.bloom_filter_types_test SELECT number AS order_key, toInt8(number) AS i8, toInt16(number) AS i16, toInt32(number) AS i32, toInt64(number) AS i64, toUInt8(number) AS u8, toUInt16(number) AS u16, toUInt32(number) AS u32, toUInt64(number) AS u64, toFloat32(number) AS f32, toFloat64(number) AS f64, toDate(number, 'Europe/Moscow') AS date, toDateTime(number, 'Europe/Moscow') AS date_time, toString(number) AS str, toFixedString(toString(number), 5) AS fixed_string FROM system.numbers LIMIT 100;

SELECT COUNT() FROM test.bloom_filter_types_test WHERE i8 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE i16 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE i32 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE i64 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE u8 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE u16 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE u32 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE u64 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE f32 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE f64 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE date = '1970-01-02' SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE date_time = toDateTime('1970-01-01 03:00:01', 'Europe/Moscow') SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE str = '1' SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.bloom_filter_types_test WHERE fixed_string = toFixedString('1', 5) SETTINGS max_rows_to_read = 12;

DROP TABLE IF EXISTS test.bloom_filter_types_test;
