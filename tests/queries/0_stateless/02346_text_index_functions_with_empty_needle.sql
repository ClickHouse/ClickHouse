-- Test the behavior of text index functions with empty needle
-- They should not match anything

-- In search{All,Any} empty needle is different from empty list:
-- See: 02346_text_index_bug86300

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (
    id Int,
    text String,
    INDEX idx_text(text) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO tab VALUES(1, 'bar'), (2, 'foo');

SELECT '-- Plain text index search functions';
SELECT count() FROM tab WHERE hasAnyTokens(text, ['']);
SELECT count() FROM tab WHERE hasAllTokens(text, ['']);
SELECT count() FROM tab WHERE hasToken(text, '');

SELECT '-- Negated text index search functions';
SELECT count() FROM tab WHERE NOT hasAnyTokens(text, ['']);
SELECT count() FROM tab WHERE NOT hasAllTokens(text, ['']);
SELECT count() FROM tab WHERE NOT hasToken(text, '');

DROP TABLE tab;

-- has() on a text index over Array(String): empty needle must fall back to a full scan (0 rows), not match everything.
DROP TABLE IF EXISTS tab_arr;
CREATE TABLE tab_arr (
    id Int,
    arr Array(String),
    INDEX idx_arr(arr) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO tab_arr VALUES(1, ['bar']), (2, ['foo']);

SELECT '-- has() with empty needle (index vs no index)';
SELECT count() FROM tab_arr WHERE has(arr, '') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab_arr WHERE has(arr, '') SETTINGS use_skip_indexes = 0;
SELECT '-- has() with present/absent needle (index)';
SELECT count() FROM tab_arr WHERE has(arr, 'foo') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab_arr WHERE has(arr, 'baz') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE tab_arr;

-- mapContainsKey / mapContainsValue (and their *Like variants) on text indexes over mapKeys/mapValues:
-- empty needle must fall back to a full scan (0 rows), not match everything.
DROP TABLE IF EXISTS tab_map;
CREATE TABLE tab_map (
    id Int,
    m Map(String, String),
    INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'array'),
    INDEX idx_values mapValues(m) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO tab_map VALUES(1, map('k1', 'v1')), (2, map('k2', 'v2'));

SELECT '-- mapContainsKey/Value with empty needle (index vs no index)';
SELECT count() FROM tab_map WHERE mapContainsKey(m, '') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab_map WHERE mapContainsKey(m, '') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_map WHERE mapContainsValue(m, '') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab_map WHERE mapContainsValue(m, '') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_map WHERE mapContainsKeyLike(m, '') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab_map WHERE mapContainsKeyLike(m, '') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_map WHERE mapContainsValueLike(m, '') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab_map WHERE mapContainsValueLike(m, '') SETTINGS use_skip_indexes = 0;

SELECT '-- mapContainsKey/Value with present/absent needle (index)';
SELECT count() FROM tab_map WHERE mapContainsKey(m, 'k1') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab_map WHERE mapContainsKey(m, 'nope') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab_map WHERE mapContainsValue(m, 'v1') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab_map WHERE mapContainsValue(m, 'nope') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE tab_map;
