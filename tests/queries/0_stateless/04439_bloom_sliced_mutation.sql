-- Tags: no-random-settings, no-random-merge-tree-settings
SET allow_experimental_bloom_sliced_index = 1;
SET apply_mutations_on_fly = 1;

DROP TABLE IF EXISTS bloom_sliced_mutation;

CREATE TABLE bloom_sliced_mutation
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

SYSTEM STOP MERGES bloom_sliced_mutation;

INSERT INTO bloom_sliced_mutation
SELECT number, if(number = 42, 'needle present', 'filler line') FROM numbers(100);

SELECT '-- hint used before mutation';
SELECT count() FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';

-- A pending on-the-fly mutation of the indexed column must disable the hint
-- (the persisted index reflects the pre-mutation data) and results must
-- include the on-the-fly updated row.
ALTER TABLE bloom_sliced_mutation UPDATE text = 'needle appeared' WHERE id = 7 SETTINGS mutations_sync = 0;

SELECT '-- hint disabled while mutation of the indexed column is pending';
SELECT count() FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT id FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';

SYSTEM START MERGES bloom_sliced_mutation;
-- Waits for this mutation and, transitively, for the pending one before it.
ALTER TABLE bloom_sliced_mutation UPDATE text = text WHERE 0 SETTINGS mutations_sync = 2;

-- The mutation rewrites the column and rebuilds the index in the mutated part,
-- so the hint is enabled again and reflects the updated data.
SELECT '-- correct results after the mutation is applied, index rebuilt, hint enabled';
SELECT count() FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT id FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';

-- MATERIALIZE INDEX on an already-materialized index is a no-op and keeps the hint enabled.
ALTER TABLE bloom_sliced_mutation MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT '-- hint still used after MATERIALIZE INDEX';
SELECT count() FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT id FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_mutation WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';

DROP TABLE bloom_sliced_mutation;
