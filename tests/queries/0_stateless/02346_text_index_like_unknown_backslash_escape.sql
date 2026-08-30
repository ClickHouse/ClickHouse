-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/115578

-- Each query is paired with the same query with `use_skip_indexes = 0`: a skip index must never change the result.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'text index';

CREATE TABLE tab (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'foo a\\b bar'), (2, 'foo ab bar'), (3, 'nothing here');

SELECT '-- LIKE';

SELECT groupArray(id) FROM tab WHERE msg LIKE '% a\\b %';
SELECT groupArray(id) FROM tab WHERE msg LIKE '% a\\b %' SETTINGS use_skip_indexes = 0;

SELECT '-- LIKE with a trailing backslash';

SELECT count() FROM tab WHERE msg LIKE 'abc\\';                               -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }
SELECT count() FROM tab WHERE msg LIKE 'abc\\' SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

DROP TABLE tab;

SELECT 'text index on Map';

CREATE TABLE tab (
    id UInt32,
    m Map(String, String),
    INDEX map_keys_idx mapKeys(m) TYPE text(tokenizer = splitByNonAlpha),
    INDEX map_values_idx mapValues(m) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, {'foo a\\b bar':'v1'}), (2, {'k2':'foo a\\b baz'}), (3, {'nothing':'here'});

SELECT '-- mapContainsKeyLike';

SELECT groupArray(id) FROM tab WHERE mapContainsKeyLike(m, '% a\\b %');
SELECT groupArray(id) FROM tab WHERE mapContainsKeyLike(m, '% a\\b %') SETTINGS use_skip_indexes = 0;

SELECT '-- mapContainsValueLike';

SELECT groupArray(id) FROM tab WHERE mapContainsValueLike(m, '% a\\b %');
SELECT groupArray(id) FROM tab WHERE mapContainsValueLike(m, '% a\\b %') SETTINGS use_skip_indexes = 0;

SELECT '-- mapContainsKeyLike with a trailing backslash';

SELECT count() FROM tab WHERE mapContainsKeyLike(m, 'abc\\'); -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

DROP TABLE tab;

SELECT 'ngrambf_v1 index';

CREATE TABLE tab (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE ngrambf_v1(3, 512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'foo a\\b bar'), (2, 'foo ab bar'), (3, 'nothing here');

SELECT '-- LIKE';

SELECT groupArray(id) FROM tab WHERE msg LIKE '% a\\b %';
SELECT groupArray(id) FROM tab WHERE msg LIKE '% a\\b %' SETTINGS use_skip_indexes = 0;

SELECT '-- NOT LIKE';

SELECT groupArray(id) FROM tab WHERE msg NOT LIKE '% a\\b %';
SELECT groupArray(id) FROM tab WHERE msg NOT LIKE '% a\\b %' SETTINGS use_skip_indexes = 0;

SELECT '-- LIKE with a trailing backslash';

SELECT count() FROM tab WHERE msg LIKE 'abc\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

DROP TABLE tab;

SELECT 'tokenbf_v1 index';

CREATE TABLE tab (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE tokenbf_v1(512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'foo a\\b bar'), (2, 'foo ab bar'), (3, 'nothing here');

SELECT '-- LIKE';

SELECT groupArray(id) FROM tab WHERE msg LIKE '% a\\b %';
SELECT groupArray(id) FROM tab WHERE msg LIKE '% a\\b %' SETTINGS use_skip_indexes = 0;

SELECT '-- LIKE with a trailing backslash';

SELECT count() FROM tab WHERE msg LIKE 'abc\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

DROP TABLE tab;

SELECT 'valid patterns still prune';

CREATE TABLE tab (
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'alpha beta'), (2, 'gamma 50%off delta'), (3, 'epsilon zeta');

SELECT '-- no backslash';

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE msg LIKE '% beta %'
) WHERE explain LIKE '%Granules: %/%';

SELECT '-- valid escape';

SELECT groupArray(id) FROM tab WHERE msg LIKE '% 50\\%off %';
SELECT groupArray(id) FROM tab WHERE msg LIKE '% 50\\%off %' SETTINGS use_skip_indexes = 0;

SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE msg LIKE '% 50\\%off %'
) WHERE explain LIKE '%Granules: %/%';

DROP TABLE tab;
