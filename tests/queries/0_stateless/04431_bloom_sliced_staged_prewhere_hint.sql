-- Tags: no-random-settings, no-random-merge-tree-settings
DROP TABLE IF EXISTS bloom_sliced_staged_prewhere_hint;
SET allow_experimental_bloom_sliced_index = 1;

CREATE TABLE bloom_sliced_staged_prewhere_hint
(
    id UInt64,
    text String,
    payload String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

INSERT INTO bloom_sliced_staged_prewhere_hint
SELECT
    number,
    multiIf(number = 42, 'rare alpha common', number % 2 = 0, 'common even', 'common odd'),
    repeat('payload', 100)
FROM numbers(100);

-- WHERE keeps the original predicate in the normal filter and installs only the hint in PREWHERE.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_staged_prewhere_hint WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Prewhere filter column:%__bloom_sliced_idx%';

SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_staged_prewhere_hint WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Filter column: hasToken(text,%alpha%';

SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_staged_prewhere_hint WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Prewhere filter column:%hasToken(text,%';

SELECT count() FROM bloom_sliced_staged_prewhere_hint WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;

-- Explicit PREWHERE still keeps the original predicate and adds the hint to the PREWHERE expression.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_staged_prewhere_hint PREWHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1, enable_multiple_prewhere_read_steps = 1)
WHERE explain LIKE '%Prewhere filter column:%__bloom_sliced_idx%hasToken%' OR explain LIKE '%Prewhere filter column:%hasToken%__bloom_sliced_idx%';

SELECT count() FROM bloom_sliced_staged_prewhere_hint PREWHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;

DROP TABLE bloom_sliced_staged_prewhere_hint;
