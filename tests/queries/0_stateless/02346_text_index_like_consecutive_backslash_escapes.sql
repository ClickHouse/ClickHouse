-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/117858

-- Each query is paired with the same query with `use_skip_indexes = 0`: a skip index must never change the result.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'text index with the ngrams tokenizer';

CREATE TABLE tab (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = ngrams(2))
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'xxab\\\\cdyy'), (2, 'xxab\\cdyy'), (3, 'nothing here');

SELECT '-- LIKE with two consecutive literal backslashes before a wildcard';

SELECT groupArray(id) FROM tab WHERE msg LIKE '%ab\\\\\\\\%cd%';
SELECT groupArray(id) FROM tab WHERE msg LIKE '%ab\\\\\\\\%cd%' SETTINGS use_skip_indexes = 0;

SELECT '-- the same pattern reached through a custom ESCAPE character';

SELECT groupArray(id) FROM tab WHERE msg LIKE '%ab\\\\%cd%' ESCAPE '!';
SELECT groupArray(id) FROM tab WHERE msg LIKE '%ab\\\\%cd%' ESCAPE '!' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'ngrambf_v1 index';

CREATE TABLE tab (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE ngrambf_v1(2, 512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'xxab\\\\cdyy'), (2, 'xxab\\cdyy'), (3, 'nothing here');

SELECT '-- LIKE with two consecutive literal backslashes before a wildcard';

SELECT groupArray(id) FROM tab WHERE msg LIKE '%ab\\\\\\\\%cd%';
SELECT groupArray(id) FROM tab WHERE msg LIKE '%ab\\\\\\\\%cd%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
