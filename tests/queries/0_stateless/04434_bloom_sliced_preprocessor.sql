-- Tags: no-random-settings, no-random-merge-tree-settings
DROP TABLE IF EXISTS bloom_sliced_preprocessor;
SET allow_experimental_bloom_sliced_index = 1;

CREATE TABLE bloom_sliced_preprocessor
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = ngrams(3), preprocessor = lower(text), bits = 4096, hashes = 3, min_hashes = 3, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

INSERT INTO bloom_sliced_preprocessor VALUES
    (1, 'Error in service'),
    (2, 'ERROR in worker'),
    (3, 'all clear');

SELECT '-- hasToken constant is preprocessed';
SELECT count() FROM bloom_sliced_preprocessor WHERE hasToken(text, 'ERROR') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_preprocessor WHERE hasToken(text, 'ERROR') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';

SELECT '-- like constant is preprocessed';
SELECT count() FROM bloom_sliced_preprocessor WHERE text LIKE '%ERROR%' SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_preprocessor WHERE text LIKE '%ERROR%' SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';

SELECT '-- ilike with lower preprocessor';
SELECT count() FROM bloom_sliced_preprocessor WHERE text ILIKE '%error%' SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_preprocessor WHERE text ILIKE '%error%' SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';

SELECT '-- hasTokenCaseInsensitive with lower preprocessor';
SELECT count() FROM bloom_sliced_preprocessor WHERE hasTokenCaseInsensitive(text, 'error') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_preprocessor WHERE hasTokenCaseInsensitive(text, 'error') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';

DROP TABLE bloom_sliced_preprocessor;
