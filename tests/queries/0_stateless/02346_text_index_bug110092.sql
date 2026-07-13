-- Bug 110092: has() and mapContainsKey/Value(Like)() with empty needles over a text index.
-- An empty needle produces zero tokens; the exact direct-read rewrite treated a zero-token
-- All-mode search as always-true and returned every row. The fix bails out of the index
-- (tokens.empty() guard in make_map_function / the has branch) so the original predicate is
-- evaluated by a full scan.
--
-- Two things matter for this regression to actually catch a reintroduction:
--  1. The wrong result only appears in the direct-read rewrite, so index-on cases pin
--     query_plan_direct_read_from_text_index = 1 (CI may randomize it to 0, which evaluates
--     the original predicate and masks the bug).
--  2. Granule pruning hides the always-true virtual column in a plain predicate, so the
--     empty-needle cases are ORed with a never-matching predicate (id = 999999, out of range).
--     Without the fix these return all 8192 rows; with the fix they return 0.

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

SET use_skip_indexes = 1;

SELECT '-- empty needle, index + direct read forced, ORed with a never-matching predicate: all 0';
SELECT count() FROM tab WHERE has(arr, '') OR id = 999999 SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE has(mp, '') OR id = 999999 SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE mapContainsKey(mp, '') OR id = 999999 SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE mapContainsValue(mp, '') OR id = 999999 SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE mapContainsKeyLike(mp, '') OR id = 999999 SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE mapContainsValueLike(mp, '') OR id = 999999 SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT '-- empty needle, no index (full scan reference): all 0';
SELECT count() FROM tab WHERE has(arr, '') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE has(mp, '') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE mapContainsKey(mp, '') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE mapContainsValue(mp, '') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE mapContainsKeyLike(mp, '') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE mapContainsValueLike(mp, '') SETTINGS use_skip_indexes = 0;

SELECT '-- present/absent needle, index + direct read forced: index still works';
SELECT count() FROM tab WHERE has(arr, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE has(arr, 'nope') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE has(mp, 'k1') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE has(mp, 'nope') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE mapContainsKey(mp, 'k1') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE mapContainsKey(mp, 'nope') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE mapContainsValue(mp, 'v1') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE mapContainsValue(mp, 'nope') SETTINGS query_plan_direct_read_from_text_index = 1;

DROP TABLE tab;
