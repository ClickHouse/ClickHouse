DROP TABLE IF EXISTS bloom_filter_nullable_index;
CREATE TABLE bloom_filter_nullable_index
    (
        order_key UInt64,
        str Nullable(String),

        INDEX idx (str) TYPE bloom_filter GRANULARITY 1
    )
    ENGINE = MergeTree() 
    ORDER BY order_key SETTINGS index_granularity = 6, index_granularity_bytes = '10Mi';

INSERT INTO bloom_filter_nullable_index VALUES (1, 'test');
INSERT INTO bloom_filter_nullable_index VALUES (2, 'test2');

SELECT 'NullableTuple with transform_null_in=0';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM bloom_filter_nullable_index) SETTINGS transform_null_in = 0;
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM bloom_filter_nullable_index) SETTINGS transform_null_in = 0;

SELECT 'NullableTuple with transform_null_in=1';

SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM bloom_filter_nullable_index) SETTINGS transform_null_in = 1; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT '1048576', str FROM bloom_filter_nullable_index) SETTINGS transform_null_in = 1; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }


SELECT 'NullableColumnFromCast with transform_null_in=0';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT cast('test', 'Nullable(String)')) SETTINGS transform_null_in = 0;

SELECT 'NullableColumnFromCast with transform_null_in=1';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT cast('test', 'Nullable(String)')) SETTINGS transform_null_in = 1;

DROP TABLE IF EXISTS nullable_string_value;
CREATE TABLE nullable_string_value (value Nullable(String)) ENGINE=TinyLog;
INSERT INTO nullable_string_value VALUES ('test');

SELECT 'NullableColumnFromTable with transform_null_in=0';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT value FROM nullable_string_value) SETTINGS transform_null_in = 0;

SELECT 'NullableColumnFromTable with transform_null_in=1';
SELECT * FROM bloom_filter_nullable_index WHERE str IN
    (SELECT value FROM nullable_string_value) SETTINGS transform_null_in = 1;

DROP TABLE nullable_string_value; 
DROP TABLE bloom_filter_nullable_index;
