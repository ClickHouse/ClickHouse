-- Tags: no-random-settings, no-random-merge-tree-settings
DROP TABLE IF EXISTS bloom_sliced_predicates;
SET allow_experimental_bloom_sliced_index = 1;

CREATE TABLE bloom_sliced_predicates
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = ngrams(3), bits = 4096, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

INSERT INTO bloom_sliced_predicates VALUES
    (1, 'service failed hard'),
    (2, 'service recovered'),
    (3, 'prefix-start marker'),
    (4, 'tail marker suffix'),
    (5, 'regex needle present'),
    (6, 'nothing interesting');

SELECT '-- hasToken with ngram tokenizer';
SELECT count() FROM bloom_sliced_predicates WHERE hasToken(text, 'failed') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_predicates WHERE hasToken(text, 'failed') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';

SELECT '-- invalid hasToken needle fails open';
SELECT count() FROM bloom_sliced_predicates WHERE hasToken(text, 'failed hard') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- like';
SELECT count() FROM bloom_sliced_predicates WHERE text LIKE '%failed%' SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_predicates WHERE text LIKE '%failed%' SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';

SELECT '-- starts ends';
SELECT count() FROM bloom_sliced_predicates WHERE startsWith(text, 'prefix-start') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_predicates WHERE endsWith(text, 'suffix') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;

SELECT '-- regex';
SELECT count() FROM bloom_sliced_predicates WHERE match(text, 'regex.*present') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_predicates WHERE match(text, 'regex.*present') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';

SELECT '-- invalid regexp raises exception instead of being silently pruned';
SELECT count() FROM bloom_sliced_predicates WHERE match(text, '(unclosed') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1; -- { serverError CANNOT_COMPILE_REGEXP }

SELECT '-- unsupported fail open';
SELECT count() FROM bloom_sliced_predicates WHERE text ILIKE '%FAILED%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_predicates WHERE text ILIKE '%FAILED%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%__bloom_sliced_idx_ilike%';
DROP TABLE bloom_sliced_predicates;
