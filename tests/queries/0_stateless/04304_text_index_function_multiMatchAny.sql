-- Tags: no-parallel-replicas, no-fasttest

-- Tests that multiMatchAny() utilizes the text index.
-- Regex search is intended to be used with the ngrams (or sparseGrams) tokenizer: the literal runs
-- extracted from each pattern are decomposed into ngrams, so the index can prune on substrings that
-- need not align with whole-word boundaries.

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
INSERT INTO tab VALUES (1, 'Well, Hello ClickHouse!'), (2, 'Well, Hello World!'), (3, 'Good Weather!'), (4, 'Say Hello!'), (5, 'Its An OLAP Database'), (6, 'True World Champion');

SELECT '-- multiMatchAny returns the same rows with and without the index';
SELECT * FROM tab WHERE multiMatchAny(str, ['Hello ClickHouse', 'OLAP']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT * FROM tab WHERE multiMatchAny(str, ['Hello ClickHouse', 'OLAP']) ORDER BY id SETTINGS use_skip_indexes = 1;

-- Read 2/6 granules (row with 'Hello ClickHouse' or 'OLAP')
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiMatchAny(str, ['Hello ClickHouse', 'OLAP']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- a pattern with alternations is handled like match()';
SELECT * FROM tab WHERE multiMatchAny(str, ['Hello (ClickHouse|World)']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT * FROM tab WHERE multiMatchAny(str, ['Hello (ClickHouse|World)']) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiMatchAny(str, ['Hello (ClickHouse|World)']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- the index is recognized by force_data_skipping_indices';
SELECT count() FROM tab WHERE multiMatchAny(str, ['OLAP', 'World']) SETTINGS force_data_skipping_indices = 'inv_idx';

SELECT '-- negation stays correct but cannot prune: the index proves a token is present, never that it is absent';
SELECT * FROM tab WHERE NOT multiMatchAny(str, ['Hello ClickHouse', 'OLAP']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT * FROM tab WHERE NOT multiMatchAny(str, ['Hello ClickHouse', 'OLAP']) ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- the "= 0" comparison form negates identically';
SELECT * FROM tab WHERE multiMatchAny(str, ['Hello ClickHouse', 'OLAP']) = 0 ORDER BY id SETTINGS use_skip_indexes = 1;

-- No granules are pruned for a negated predicate (6/6), unlike the positive predicate (2/6)
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE NOT multiMatchAny(str, ['Hello ClickHouse', 'OLAP']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- the index is still recognized by force_data_skipping_indices (it is read, just prunes nothing)';
SELECT count() FROM tab WHERE NOT multiMatchAny(str, ['Hello ClickHouse', 'OLAP']) SETTINGS force_data_skipping_indices = 'inv_idx';

SELECT '-- a catch-all pattern disables pruning, but results stay correct';
SELECT * FROM tab WHERE multiMatchAny(str, ['OLAP', '.*']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT * FROM tab WHERE multiMatchAny(str, ['OLAP', '.*']) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiMatchAny(str, ['OLAP', '.*']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- a catch-all pattern cannot satisfy force_data_skipping_indices';
SELECT count() FROM tab WHERE multiMatchAny(str, ['OLAP', '.*']) SETTINGS force_data_skipping_indices = 'inv_idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab;
