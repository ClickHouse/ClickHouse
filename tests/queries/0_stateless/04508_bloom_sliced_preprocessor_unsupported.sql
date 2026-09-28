-- Tags: no-random-settings, no-random-merge-tree-settings
-- A preprocessor that is not a pure case fold (here `substring`) is not containment-preserving:
-- the index stores tokens of the transformed text while the predicate is evaluated on the raw
-- column. Case-sensitive predicates may still be lowered because the index stores per-chunk
-- tombstone Bloom filters of the raw tokens the preprocessor destroyed: a probe that hits a
-- tombstone opens the chunk instead of pruning it, so results must stay correct (no false
-- negatives). Case-insensitive predicates (`ILIKE`, `hasTokenCaseInsensitive`) still require a
-- pure case-fold preprocessor - tombstones certify nothing about case variants of stored tokens -
-- so they must not be lowered at all (total open fallback, the index does not appear in the plan).
DROP TABLE IF EXISTS bloom_sliced_unsupported;
DROP TABLE IF EXISTS bloom_sliced_unsupported_no_index;
SET allow_experimental_bloom_sliced_index = 1;

CREATE TABLE bloom_sliced_unsupported
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = ngrams(3), preprocessor = substring(text, 2, 3), bits = 4096, hashes = 3, min_hashes = 3, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

CREATE TABLE bloom_sliced_unsupported_no_index
(
    id UInt64,
    text String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

INSERT INTO bloom_sliced_unsupported VALUES
    (1, 'hello world'),
    (2, 'sliced bloom'),
    (3, 'plain data');

INSERT INTO bloom_sliced_unsupported_no_index VALUES
    (1, 'hello world'),
    (2, 'sliced bloom'),
    (3, 'plain data');

SELECT '-- hasToken must not lose matches, hint on and off';
SELECT count() FROM bloom_sliced_unsupported WHERE hasToken(text, 'world') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_unsupported WHERE hasToken(text, 'world') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
SELECT count() FROM bloom_sliced_unsupported_no_index WHERE hasToken(text, 'world');

SELECT '-- LIKE must not lose matches, hint on and off';
SELECT count() FROM bloom_sliced_unsupported WHERE text LIKE '%world%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_unsupported WHERE text LIKE '%world%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
SELECT count() FROM bloom_sliced_unsupported_no_index WHERE text LIKE '%world%';

SELECT '-- case-sensitive predicates engage the index (tombstone open fallback), hint on and off';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_unsupported WHERE hasToken(text, 'world') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_unsupported WHERE hasToken(text, 'world') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0)
WHERE explain LIKE '%Name: idx%';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_unsupported WHERE text LIKE '%world%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_unsupported WHERE text LIKE '%world%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0)
WHERE explain LIKE '%Name: idx%';

SELECT '-- case-insensitive predicates must not lose matches';
SELECT count() FROM bloom_sliced_unsupported WHERE text ILIKE '%WORLD%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_unsupported WHERE hasTokenCaseInsensitive(text, 'WORLD') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_unsupported_no_index WHERE text ILIKE '%WORLD%';

SELECT '-- case-insensitive predicates keep the case-fold gate: the index is not used at all';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_unsupported WHERE text ILIKE '%WORLD%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_unsupported WHERE hasTokenCaseInsensitive(text, 'WORLD') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';

DROP TABLE bloom_sliced_unsupported;
DROP TABLE bloom_sliced_unsupported_no_index;
