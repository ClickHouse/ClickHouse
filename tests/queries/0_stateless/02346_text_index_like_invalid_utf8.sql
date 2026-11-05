-- A LIKE pattern with an invalid UTF-8 sequence must be tokenized byte by byte, like the stored values,
-- so a separator or a wildcard after an invalid lead byte is not swallowed into the token.

-- Each query is paired with the same query with `use_skip_indexes = 0`: a skip index must never change the result.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'tokensForLikePattern';

SELECT arrayMap(x -> hex(x), tokensForLikePattern('\xF0(\x8C\xBC')), arrayMap(x -> hex(x), tokens('\xF0(\x8C\xBC'));
SELECT arrayMap(x -> hex(x), tokensForLikePattern('a \xC3%b'));

SELECT 'text index';

CREATE TABLE tab (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, '\xF0(\x8C\xBC'), (2, 'a \xC3XYZb'), (3, 'nothing here');

SELECT '-- separator after an invalid lead byte';

SELECT groupArray(id) FROM tab WHERE msg LIKE '\xF0(\x8C\xBC';
SELECT groupArray(id) FROM tab WHERE msg LIKE '\xF0(\x8C\xBC' SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE msg LIKE '\xF0(\x8C\xBC'
) WHERE explain LIKE '%Granules: %/%';

SELECT '-- wildcard after an invalid lead byte';

SELECT groupArray(id) FROM tab WHERE msg LIKE 'a \xC3%b';
SELECT groupArray(id) FROM tab WHERE msg LIKE 'a \xC3%b' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'tokenbf_v1 index';

CREATE TABLE tab (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE tokenbf_v1(512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, '\xF0(\x8C\xBC'), (2, 'a \xC3XYZb'), (3, 'nothing here');

SELECT '-- separator after an invalid lead byte';

SELECT groupArray(id) FROM tab WHERE msg LIKE '\xF0(\x8C\xBC';
SELECT groupArray(id) FROM tab WHERE msg LIKE '\xF0(\x8C\xBC' SETTINGS use_skip_indexes = 0;

SELECT '-- wildcard after an invalid lead byte';

SELECT groupArray(id) FROM tab WHERE msg LIKE 'a \xC3%b';
SELECT groupArray(id) FROM tab WHERE msg LIKE 'a \xC3%b' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
