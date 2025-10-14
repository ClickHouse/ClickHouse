-- Tags: no-parallel-replicas

-- Tests that text indexes can be build on and used with Array columns.

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    arr Array(String),
    arr_fixed Array(FixedString(3)),
    INDEX array_idx(arr) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX array_fixed_idx(arr_fixed) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
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
SELECT count() FROM tab WHERE has(arr, 'def');

SELECT '-- with FixedString';
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('foo', 3));
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('bar', 3));
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('baz', 3));
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('def', 3));

SELECT '-- Check that the text index actually gets used (String)';

DROP VIEW IF EXISTS explain_index_has;
CREATE VIEW explain_index_has AS (
    SELECT trimLeft(explain) AS explain FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM tab WHERE (
            CASE
                WHEN {use_idx_fixed:boolean} = 1 THEN has(arr_fixed, {filter:FixedString(3)})
                ELSE has(arr, {filter:String})
            END
        )
    )
    WHERE explain LIKE '%Description:%' OR explain LIKE '%Granules:%'
    LIMIT 1, 2
);

SELECT '-- -- value exists only in 1024 granules';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'abc');

SELECT '-- -- value exists only in 2048 granules';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'baz');

SELECT '-- -- value exists only in 3072 granules';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'foo');

SELECT '-- -- value exists only in 3072 granules';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'bar');

SELECT '-- -- value does not exist in granules';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'def');

SELECT '-- Check that the text index actually gets used (FixedString)';

SELECT '-- -- value exists only in 1024 granules';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = toFixedString('abc', 3));

SELECT '-- -- value exists only in 2048 granules';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = toFixedString('baz', 3));

SELECT '-- -- value exists only in 3072 granules';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = toFixedString('foo', 3));

SELECT '-- -- value exists only in 3072 granules';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = toFixedString('bar', 3));

SELECT '-- -- value does not exist in granules';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = toFixedString('def', 3));

DROP VIEW explain_index_has;
DROP TABLE tab;
