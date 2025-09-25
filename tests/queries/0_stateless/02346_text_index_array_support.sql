-- Tags: no-parallel-replicas

SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 0; --- for EXPLAIN indexes = 1 <query>

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    arr Array(String),
    arr_fixed Array(FixedString(3)),
    INDEX array_idx(arr) TYPE text(tokenizer = 'default') GRANULARITY 1,
    INDEX array_fixed_idx(arr_fixed) TYPE text(tokenizer = 'default') GRANULARITY 1,
)
ENGINE = MergeTree()
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT number, ['abc'], ['abc'] FROM numbers(1024);
INSERT INTO tab SELECT number, ['foo'], ['foo'] FROM numbers(1024);
INSERT INTO tab SELECT number, ['bar'], ['bar'] FROM numbers(1024);
INSERT INTO tab SELECT number, ['foo', 'bar'], ['foo', 'bar'] FROM numbers(1024);
INSERT INTO tab SELECT number, ['foo', 'baz'], ['foo', 'baz'] FROM numbers(1024);
INSERT INTO tab SELECT number, ['bar', 'baz'], ['bar', 'baz'] FROM numbers(1024);

SELECT 'has support';

SELECT '-- with String';
SELECT count() FROM tab WHERE has(arr, 'foo');
SELECT count() FROM tab WHERE has(arr, 'bar');
SELECT count() FROM tab WHERE has(arr, 'baz');

SELECT '-- with FixedString';
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('foo', 3));
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('bar', 3));
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('baz', 3));

SELECT '-- index analyzer with String';
SELECT 'value exists only in 1024 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr, 'abc')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
SELECT 'value exists only in 2048 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr, 'baz')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
SELECT 'value exists only in 3072 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr, 'foo')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
SELECT 'value exists only in 3072 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr, 'bar')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
SELECT 'value does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr, 'def')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- index analyzer with FixedString';
SELECT 'value exists only in 1024 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('abc', 3))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
SELECT 'value exists only in 2048 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('baz', 3))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
SELECT 'value exists only in 3072 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('foo', 3))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
SELECT 'value exists only in 3072 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('bar', 3))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
SELECT 'value does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('def', 3))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;
