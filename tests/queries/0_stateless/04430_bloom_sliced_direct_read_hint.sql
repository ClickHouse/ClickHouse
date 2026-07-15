-- Tags: no-random-settings, no-random-merge-tree-settings
DROP TABLE IF EXISTS bloom_sliced_direct_read_hint;
SET allow_experimental_bloom_sliced_index = 1;

CREATE TABLE bloom_sliced_direct_read_hint
(
    id UInt64,
    text String,
    payload String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

INSERT INTO bloom_sliced_direct_read_hint
SELECT
    number,
    multiIf(number = 42, 'rare alpha common', number % 2 = 0, 'common even', 'common odd'),
    repeat('payload', 10)
FROM numbers(100);

SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_direct_read_hint PREWHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%__bloom_sliced_idx%';

SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_direct_read_hint PREWHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%hasToken%';

SELECT count() FROM bloom_sliced_direct_read_hint PREWHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;

SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_direct_read_hint WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%__bloom_sliced_idx%';

SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_direct_read_hint WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%hasToken%';

SELECT count() FROM bloom_sliced_direct_read_hint WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;

-- bloom_sliced staged hints are controlled by the bloom_sliced setting, not the text-index direct-read setting.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_direct_read_hint WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 0, query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%__bloom_sliced_idx%';

SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_direct_read_hint WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1, query_plan_direct_read_from_bloom_sliced_index = 0)
WHERE explain LIKE '%__bloom_sliced_idx%';

-- Dense/nonselective hints should be an all-true reader-side no-op when index analysis did not keep a row-selective granule.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_direct_read_hint PREWHERE hasToken(text, 'common') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%__bloom_sliced_idx%';

SELECT count() FROM bloom_sliced_direct_read_hint PREWHERE hasToken(text, 'common') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;

DROP TABLE bloom_sliced_direct_read_hint;
