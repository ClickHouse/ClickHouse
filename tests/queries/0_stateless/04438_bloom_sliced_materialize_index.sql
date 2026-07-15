-- Tags: no-random-settings, no-random-merge-tree-settings
SET allow_experimental_bloom_sliced_index = 1;

DROP TABLE IF EXISTS bloom_sliced_materialize;

CREATE TABLE bloom_sliced_materialize
(
    id UInt64,
    text String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

INSERT INTO bloom_sliced_materialize
SELECT number, if(number = 42, 'needle present', 'filler line') FROM numbers(100);

ALTER TABLE bloom_sliced_materialize
    ADD INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1;

SELECT '-- before materialization: correct result, no hint for non-materialized parts';
SELECT count() FROM bloom_sliced_materialize WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_materialize WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%__bloom_sliced_idx%';

ALTER TABLE bloom_sliced_materialize MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT '-- after materialization: correct result, pruning and hint active';
SELECT count() FROM bloom_sliced_materialize WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT id FROM bloom_sliced_materialize WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_materialize WHERE hasToken(text, 'needle'))
WHERE explain LIKE '%Granules: 1/10%';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_materialize WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Prewhere filter column:%__bloom_sliced_idx%';

DROP TABLE bloom_sliced_materialize;
