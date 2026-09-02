-- https://github.com/ClickHouse/ClickHouse/issues/42460
DROP TABLE IF EXISTS bloom_filter_nullable_index__fuzz_0;
CREATE TABLE bloom_filter_nullable_index__fuzz_0
(
    `order_key` UInt64,
    `str` Nullable(String),
    INDEX idx str TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY order_key SETTINGS index_granularity = 6, index_granularity_bytes = '10Mi';

INSERT INTO bloom_filter_nullable_index__fuzz_0 VALUES (1, 'test');
INSERT INTO bloom_filter_nullable_index__fuzz_0 VALUES (2, 'test2');

DROP TABLE IF EXISTS bloom_filter_nullable_index__fuzz_1;
CREATE TABLE bloom_filter_nullable_index__fuzz_1
(
    `order_key` UInt64,
    `str` String,
    INDEX idx str TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY order_key SETTINGS index_granularity = 6, index_granularity_bytes = '10Mi';

INSERT INTO bloom_filter_nullable_index__fuzz_0 VALUES (1, 'test');
INSERT INTO bloom_filter_nullable_index__fuzz_0 VALUES (2, 'test2');

DROP TABLE IF EXISTS nullable_string_value__fuzz_2;
CREATE TABLE nullable_string_value__fuzz_2 (`value` LowCardinality(String)) ENGINE = TinyLog;
INSERT INTO nullable_string_value__fuzz_2 VALUES ('test');

SELECT * FROM bloom_filter_nullable_index__fuzz_0 WHERE str IN (SELECT value FROM nullable_string_value__fuzz_2);
SELECT * FROM bloom_filter_nullable_index__fuzz_1 WHERE str IN (SELECT value FROM nullable_string_value__fuzz_2);
