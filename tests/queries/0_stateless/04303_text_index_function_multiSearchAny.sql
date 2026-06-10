-- Tags: no-parallel-replicas

-- Tests that multiSearchAny() and multiSearchAnyUTF8() utilize the text index.
-- Substring search is intended to be used with the ngrams (or sparseGrams) tokenizer: every needle
-- is decomposed into ngrams, so the index can prune on arbitrary substrings instead of whole tokens.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX inv_idx(str) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO tab VALUES (1, 'Well, Hello ClickHouse !'), (2, 'Well, Hello World !'), (3, 'Good Weather !'), (4, 'Say Hello !'), (5, 'Its An OLAP Database'), (6, 'True World Champion');

SELECT '-- multiSearchAny returns the same rows with and without the index';
SELECT * FROM tab WHERE multiSearchAny(str, ['ClickHouse', 'World']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT * FROM tab WHERE multiSearchAny(str, ['ClickHouse', 'World']) ORDER BY id SETTINGS use_skip_indexes = 1;

-- Read 3/6 granules (rows with 'ClickHouse' or 'World')
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiSearchAny(str, ['ClickHouse', 'World']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- multiSearchAnyUTF8 behaves identically';
SELECT * FROM tab WHERE multiSearchAnyUTF8(str, ['ClickHouse', 'World']) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiSearchAnyUTF8(str, ['ClickHouse', 'World']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- a needle that is absent everywhere prunes all granules';
SELECT * FROM tab WHERE multiSearchAny(str, ['Nonexistent']) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiSearchAny(str, ['Nonexistent']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- the index is recognized by force_data_skipping_indices';
SELECT count() FROM tab WHERE multiSearchAny(str, ['ClickHouse', 'World']) SETTINGS force_data_skipping_indices = 'inv_idx';

SELECT '-- a single word prunes with ngrams (its sub-word grams are indexed), unlike splitByNonAlpha';
SELECT * FROM tab WHERE multiSearchAny(str, ['Hello']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT * FROM tab WHERE multiSearchAny(str, ['Hello']) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiSearchAny(str, ['Hello']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- negation stays correct but cannot prune: the index proves a token is present, never that it is absent';
SELECT * FROM tab WHERE NOT multiSearchAny(str, ['ClickHouse', 'World']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT * FROM tab WHERE NOT multiSearchAny(str, ['ClickHouse', 'World']) ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- the "= 0" comparison form negates identically';
SELECT * FROM tab WHERE multiSearchAny(str, ['ClickHouse', 'World']) = 0 ORDER BY id SETTINGS use_skip_indexes = 1;

-- No granules are pruned for a negated predicate (6/6), unlike the positive predicate (3/6)
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE NOT multiSearchAny(str, ['ClickHouse', 'World']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- the index is still recognized by force_data_skipping_indices (it is read, just prunes nothing)';
SELECT count() FROM tab WHERE NOT multiSearchAny(str, ['ClickHouse', 'World']) SETTINGS force_data_skipping_indices = 'inv_idx';

SELECT '-- an empty needle array is always false';
SELECT count() FROM tab WHERE multiSearchAny(str, CAST([], 'Array(String)')) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE multiSearchAny(str, CAST([], 'Array(String)')) SETTINGS use_skip_indexes = 1;

DROP TABLE tab;
