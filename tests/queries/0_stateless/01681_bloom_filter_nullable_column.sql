CREATE DATABASE 01681_bloom_filter_nullable_column;

DROP TABLE IF EXISTS 01681_bloom_filter_nullable_column.bloom_filter_nullable_index;

CREATE TABLE 01681_bloom_filter_nullable_column.bloom_filter_nullable_index
    (
        order_key UInt64,
        str Nullable(String),

        INDEX idx (str) TYPE bloom_filter GRANULARITY 1
    )
    ENGINE = MergeTree() 
    ORDER BY order_key SETTINGS index_granularity = 6;

INSERT INTO 01681_bloom_filter_nullable_column.bloom_filter_nullable_index VALUES (1, 'test');
INSERT INTO 01681_bloom_filter_nullable_column.bloom_filter_nullable_index VALUES (2, 'test2');

SELECT 'NullableTuple with transform_null_in=0';
SELECT * FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index) SETTINGS transform_null_in = 0;
SELECT * FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index) SETTINGS transform_null_in = 0;

SELECT 'NullableTuple with transform_null_in=1';

SELECT * FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index) SETTINGS transform_null_in = 1; -- { serverError 20 }

SELECT * FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index) SETTINGS transform_null_in = 1; -- { serverError 20 }


SELECT 'NullableColumnFromCast with transform_null_in=0';
SELECT * FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index WHERE str IN
    (SELECT cast('test', 'Nullable(String)')) SETTINGS transform_null_in = 0;

SELECT 'NullableColumnFromCast with transform_null_in=1';
SELECT * FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index WHERE str IN
    (SELECT cast('test', 'Nullable(String)')) SETTINGS transform_null_in = 1;

CREATE TABLE 01681_bloom_filter_nullable_column.nullable_string_value (value Nullable(String)) ENGINE=TinyLog;
INSERT INTO 01681_bloom_filter_nullable_column.nullable_string_value VALUES ('test');

SELECT 'NullableColumnFromTable with transform_null_in=0';
SELECT * FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index WHERE str IN
    (SELECT value FROM 01681_bloom_filter_nullable_column.nullable_string_value) SETTINGS transform_null_in = 0;

SELECT 'NullableColumnFromTable with transform_null_in=1';
SELECT * FROM 01681_bloom_filter_nullable_column.bloom_filter_nullable_index WHERE str IN
    (SELECT value FROM 01681_bloom_filter_nullable_column.nullable_string_value) SETTINGS transform_null_in = 1;

DROP TABLE 01681_bloom_filter_nullable_column.nullable_string_value; 

DROP TABLE 01681_bloom_filter_nullable_column.bloom_filter_nullable_index;
DROP DATABASE 01681_bloom_filter_nullable_column;
