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

-- No empty strings anywhere in the data.
INSERT INTO tab
SELECT
    number,
    [concat('tok', toString(number))],
    map(concat('k', toString(number)), concat('v', toString(number)))
FROM numbers(8192);

SET use_skip_indexes = 0;
SELECT count() FROM tab WHERE has(arr, '');
SELECT count() FROM tab WHERE mapContainsKey(mp, '');
SELECT count() FROM tab WHERE mapContainsValue(mp, '');

SET use_skip_indexes = 1;
SELECT count() FROM tab WHERE has(arr, '');
SELECT count() FROM tab WHERE mapContainsKey(mp, '');
SELECT count() FROM tab WHERE mapContainsValue(mp, '');

DROP TABLE tab;
