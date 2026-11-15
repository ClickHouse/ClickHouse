-- Constant columns do not contribute to the hash partition key, including constant NULL columns.
SET max_threads = 4;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
SET allow_parallel_distinct = 1;

SELECT count() FROM (SELECT DISTINCT 1, CAST(NULL AS Nullable(UInt64)) FROM numbers_mt(10000));
SELECT count() FROM (SELECT DISTINCT 1, number % 10, CAST(NULL AS Nullable(UInt64)) FROM numbers_mt(10000));
SELECT count() FROM (SELECT DISTINCT * FROM (SELECT 1, number % 10, CAST(NULL AS Nullable(UInt64)) FROM numbers_mt(10000)));
SELECT count() FROM (SELECT DISTINCT number FROM numbers_mt(0));

-- Set-building deduplication retains the non-constant partition key and skips constant NULL keys
-- only when the set consumer also drops NULLs.
SET allow_creating_set_partitions_independently = 1;
SET enable_parallel_replicas = 0;
CREATE TABLE distinct_key_columns (k UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY k % 4;
INSERT INTO distinct_key_columns SELECT number FROM numbers(40000);

SELECT countIf(explain LIKE '%DistinctTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT number FROM numbers(10) WHERE (number, NULL) IN (SELECT k, NULL FROM distinct_key_columns));
SELECT count() FROM numbers(10) WHERE (number, NULL) IN (SELECT k, NULL FROM distinct_key_columns) SETTINGS transform_null_in = 0;
SELECT count() FROM numbers(10) WHERE (number, NULL) IN (SELECT k, NULL FROM distinct_key_columns) SETTINGS transform_null_in = 1;
SELECT count() FROM numbers(10) WHERE (number, 7) IN (SELECT k, 7 FROM distinct_key_columns) SETTINGS transform_null_in = 0;

DROP TABLE distinct_key_columns;
