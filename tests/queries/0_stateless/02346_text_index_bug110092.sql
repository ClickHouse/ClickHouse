-- Bug 110092: has() and mapContainsKey/Value() with empty needles

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    arr Array(String),
    mp Map(String, String),
    INDEX a_text arr TYPE text(tokenizer = 'array'),
    INDEX mk_text mapKeys(mp) TYPE text(tokenizer = 'array'),
    INDEX mv_text mapValues(mp) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

-- No empty strings anywhere in the data; id is always < 8192.
INSERT INTO tab
SELECT
    number,
    [concat('tok', toString(number))],
    map(concat('k', toString(number)), concat('v', toString(number)))
FROM numbers(8192);


SELECT '-- empty needle, index lookup';
SET use_skip_indexes = 1;
-- The wrong-results path is only reached when the direct read from the text index is enabled, which
-- otherwise gets randomized by clickhouse-test. Force it on so the regression is deterministic.
SET query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE has(arr, '');
SELECT count() FROM tab WHERE has(mp, '');
SELECT count() FROM tab WHERE mapContainsKey(mp, '');
SELECT count() FROM tab WHERE mapContainsValue(mp, '');
SELECT count() FROM tab WHERE mapContainsKeyLike(mp, '');
SELECT count() FROM tab WHERE mapContainsValueLike(mp, '');
SET query_plan_direct_read_from_text_index = default;
SET use_skip_indexes = default;

SELECT '-- empty needle, no index';
SET use_skip_indexes = 0;
SELECT count() FROM tab WHERE has(arr, '');
SELECT count() FROM tab WHERE has(mp, '');
SELECT count() FROM tab WHERE mapContainsKey(mp, '');
SELECT count() FROM tab WHERE mapContainsValue(mp, '');
SELECT count() FROM tab WHERE mapContainsKeyLike(mp, '');
SELECT count() FROM tab WHERE mapContainsValueLike(mp, '');
SET use_skip_indexes = default;

DROP TABLE tab;
