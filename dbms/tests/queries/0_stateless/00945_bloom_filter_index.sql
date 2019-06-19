DROP TABLE IF EXISTS test.single_column_bloom_filter;

SET allow_experimental_data_skipping_indices = 1;

CREATE TABLE test.single_column_bloom_filter (u64 UInt64, i32 Int32, i64 UInt64, INDEX idx (i32) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 6;

INSERT INTO test.single_column_bloom_filter SELECT number AS u64, number AS i32, number AS i64 FROM system.numbers LIMIT 100;

SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 = 1 SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i32) = (1, 2) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i64) = (1, 1) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i64, (i64, i32)) = (1, (1, 1)) SETTINGS max_rows_to_read = 6;

SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 = 1 SETTINGS max_rows_to_read = 5; -- { serverError 158 }
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i32) = (1, 2) SETTINGS max_rows_to_read = 5; -- { serverError 158 }
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i64) = (1, 1) SETTINGS max_rows_to_read = 5; -- { serverError 158 }
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i64, (i64, i32)) = (1, (1, 1)) SETTINGS max_rows_to_read = 5; -- { serverError 158 }

SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 IN (1, 2) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i32) IN ((1, 2), (2, 3)) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i64) IN ((1, 1), (2, 2)) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i64, (i64, i32)) = (1, (1, 1)) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 IN (SELECT arrayJoin([1, 2])) SETTINGS max_rows_to_read = 6;
SELECT COUNT() FROM test.single_column_bloom_filter WHERE (i32, i32) IN (SELECT arrayJoin([(1, 1), (2, 2)])) SETTINGS max_rows_to_read = 6;
WITH (1, 2) AS liter_prepared_set SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 IN liter_prepared_set SETTINGS max_rows_to_read = 6;

SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 IN (1, 2) SETTINGS max_rows_to_read = 5; -- { serverError 158 }
SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 IN (SELECT arrayJoin([1, 2])) SETTINGS max_rows_to_read = 5; -- { serverError 158 }
WITH (1, 2) AS liter_prepared_set SELECT COUNT() FROM test.single_column_bloom_filter WHERE i32 IN liter_prepared_set SETTINGS max_rows_to_read = 5; -- { serverError 158 }

DROP TABLE IF EXISTS test.single_column_bloom_filter;
