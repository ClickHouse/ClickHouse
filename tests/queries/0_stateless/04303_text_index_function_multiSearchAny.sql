-- Tags: no-parallel-replicas

-- Tests that multiSearchAny() and multiSearchAnyUTF8() utilize the text index.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX inv_idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO tab VALUES (1, 'Well, Hello ClickHouse !'), (2, 'Well, Hello World !'), (3, 'Good Weather !'), (4, 'Say Hello !'), (5, 'Its An OLAP Database'), (6, 'True World Champion');

SELECT '-- multiSearchAny returns the same rows with and without the index';
SELECT * FROM tab WHERE multiSearchAny(str, [' ClickHouse ', ' World ']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT * FROM tab WHERE multiSearchAny(str, [' ClickHouse ', ' World ']) ORDER BY id SETTINGS use_skip_indexes = 1;

-- Read 3/6 granules (rows with ' ClickHouse ' or ' World ')
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiSearchAny(str, [' ClickHouse ', ' World ']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- multiSearchAnyUTF8 behaves identically';
SELECT * FROM tab WHERE multiSearchAnyUTF8(str, [' ClickHouse ', ' World ']) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiSearchAnyUTF8(str, [' ClickHouse ', ' World ']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- a needle that is absent everywhere prunes all granules';
SELECT * FROM tab WHERE multiSearchAny(str, [' Nonexistent ']) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiSearchAny(str, [' Nonexistent ']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- the index is recognized by force_data_skipping_indices';
SELECT count() FROM tab WHERE multiSearchAny(str, [' ClickHouse ', ' World ']) SETTINGS force_data_skipping_indices = 'inv_idx';

SELECT '-- a single-word needle has no complete token, so it cannot prune, but results stay correct';
SELECT * FROM tab WHERE multiSearchAny(str, ['Hello']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT * FROM tab WHERE multiSearchAny(str, ['Hello']) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab WHERE multiSearchAny(str, ['Hello']) ORDER BY id
)
WHERE explain LIKE '%Granules: %';

SELECT '-- an empty needle array is always false';
SELECT count() FROM tab WHERE multiSearchAny(str, CAST([], 'Array(String)')) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE multiSearchAny(str, CAST([], 'Array(String)')) SETTINGS use_skip_indexes = 1;

DROP TABLE tab;
