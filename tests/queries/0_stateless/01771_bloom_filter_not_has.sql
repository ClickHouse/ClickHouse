-- Tags: no-parallel, long
DROP TABLE IF EXISTS bloom_filter_null_array;
CREATE TABLE bloom_filter_null_array (v Array(Int32), INDEX idx v TYPE bloom_filter GRANULARITY 3) ENGINE = MergeTree() ORDER BY v SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO bloom_filter_null_array SELECT [number] FROM numbers(10000000);
SELECT COUNT() FROM bloom_filter_null_array;
SELECT COUNT() FROM bloom_filter_null_array WHERE has(v, 0);
SELECT COUNT() FROM bloom_filter_null_array WHERE not has(v, 0);
DROP TABLE bloom_filter_null_array;
