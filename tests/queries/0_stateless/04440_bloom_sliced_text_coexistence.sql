-- Tags: no-random-settings, no-random-merge-tree-settings
SET allow_experimental_bloom_sliced_index = 1;

DROP TABLE IF EXISTS bloom_sliced_text_coexistence;

CREATE TABLE bloom_sliced_text_coexistence
(
    id UInt64,
    text String,
    INDEX text_idx text TYPE text(tokenizer = splitByNonAlpha),
    INDEX bloom_idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

INSERT INTO bloom_sliced_text_coexistence
SELECT number, if(number = 42, 'needle present', 'filler line') FROM numbers(100);

SELECT '-- correct results with both indexes';
SELECT count() FROM bloom_sliced_text_coexistence WHERE hasToken(text, 'needle');
SELECT id FROM bloom_sliced_text_coexistence WHERE hasToken(text, 'needle');

SELECT '-- text index wins: text virtual column used, bloom_sliced hint not installed';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_text_coexistence WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_text\_index%';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_text_coexistence WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced%';

SELECT count() FROM bloom_sliced_text_coexistence WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_direct_read_from_bloom_sliced_index = 1;

DROP TABLE bloom_sliced_text_coexistence;
