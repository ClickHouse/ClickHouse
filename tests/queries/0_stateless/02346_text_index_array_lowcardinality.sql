-- Tags: no-parallel-replicas, no-azure-blob-storage

-- Tests that text indexes can be created on and used with Array(LowCardinality(String)) and
-- Array(LowCardinality(FixedString)) columns.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    arr Array(LowCardinality(String)),
    arr_fixed Array(LowCardinality(FixedString(3))),
    INDEX arr_idx(arr) TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX arr_fixed_idx(arr_fixed) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES tab;

-- Three separate inserts produce three separate parts; only one part has 'baz'.
INSERT INTO tab SELECT number, ['foo'],        ['foo']        FROM numbers(512);
INSERT INTO tab SELECT number, ['bar'],        ['bar']        FROM numbers(512);
INSERT INTO tab SELECT number, ['foo', 'baz'], ['foo', 'baz'] FROM numbers(512);

SELECT '-- Correctness (String)';
SELECT count() FROM tab WHERE has(arr, 'foo');  -- 1024
SELECT count() FROM tab WHERE has(arr, 'bar');  -- 512
SELECT count() FROM tab WHERE has(arr, 'baz');  -- 512
SELECT count() FROM tab WHERE has(arr, 'xyz');  -- 0

SELECT '-- Correctness (FixedString)';
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('foo', 3));  -- 1024
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('bar', 3));  -- 512
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('baz', 3));  -- 512
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('xyz', 3));  -- 0

SELECT '-- Index usage (String)';

SELECT '-- -- foo matches 1024/1536 granules';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE has(arr, 'foo')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Granules:%'
LIMIT 1, 2;

SELECT '-- -- baz matches 512/1536 granules';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE has(arr, 'baz')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Granules:%'
LIMIT 1, 2;

SELECT '-- -- xyz matches 0/1536 granules';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE has(arr, 'xyz')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Granules:%'
LIMIT 1, 2;

SELECT '-- Index usage (FixedString)';

SELECT '-- -- foo matches 1024/1536 granules';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('foo', 3))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Granules:%'
LIMIT 1, 2;

SELECT '-- -- baz matches 512/1536 granules';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('baz', 3))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Granules:%'
LIMIT 1, 2;

SELECT '-- -- xyz matches 0/1536 granules';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('xyz', 3))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Granules:%'
LIMIT 1, 2;

DROP TABLE tab;
