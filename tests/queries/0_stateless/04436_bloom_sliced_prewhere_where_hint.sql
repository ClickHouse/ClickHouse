-- Tags: no-random-settings, no-random-merge-tree-settings
SET allow_experimental_bloom_sliced_index = 1;

DROP TABLE IF EXISTS bloom_sliced_prewhere_where_hint;

CREATE TABLE bloom_sliced_prewhere_where_hint
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

-- Rows 42 and 47 contain the token. The explicit PREWHERE `id >= 45` cuts granule 40..49 mid-granule,
-- so row 42 is read from disk and must be filtered out by the user PREWHERE predicate itself, not by
-- primary-key mark pruning. If installing the token hint dropped the user PREWHERE, row 42 would leak
-- into the result.
INSERT INTO bloom_sliced_prewhere_where_hint
SELECT number, if(number IN (42, 47), 'needle present', 'filler line') FROM numbers(100);

SELECT '-- explicit non-token PREWHERE + token WHERE, hint enabled';
SELECT count() FROM bloom_sliced_prewhere_where_hint PREWHERE id >= 45 WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT id FROM bloom_sliced_prewhere_where_hint PREWHERE id >= 45 WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;

SELECT '-- explicit non-token PREWHERE + token WHERE, hint disabled';
SELECT count() FROM bloom_sliced_prewhere_where_hint PREWHERE id >= 45 WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
SELECT id FROM bloom_sliced_prewhere_where_hint PREWHERE id >= 45 WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;

SELECT '-- the hint is prepended and the user PREWHERE predicate survives';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_prewhere_where_hint PREWHERE id >= 45 WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE (explain LIKE '%Prewhere filter column:%id >= 45%\_\_bloom\_sliced\_idx%' OR explain LIKE '%Prewhere filter column:%\_\_bloom\_sliced\_idx%id >= 45%');

DROP TABLE bloom_sliced_prewhere_where_hint;
