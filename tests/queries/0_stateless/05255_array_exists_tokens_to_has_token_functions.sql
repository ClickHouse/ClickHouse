-- Tags: no-parallel-replicas
-- `arrayExists(x -> f(x, c), tokens(s))` is rewritten to `hasTokenLike` or `hasTokenMatch` over `s`, which the text index
-- on `s` can answer. The rewrite must not change any result, with or without the index.

SET enable_analyzer = 1;
SET optimize_rewrite_array_exists_to_has = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_plain;

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

CREATE TABLE tab_plain (id UInt32, msg String) ENGINE = MergeTree ORDER BY id;

-- 128 granules of 8 rows.
INSERT INTO tab SELECT
    number,
    multiIf(
        number < 8, 'Payment charged twice',
        number >= 400 AND number < 408, 'recharge failed for order 12345',
        number >= 1016, 'Charging station 123456 is busy',
        number % 2 = 0, 'user login ok',
        'user logout ok')
FROM numbers(1024);

INSERT INTO tab_plain SELECT * FROM tab;

SELECT '-- the same rows with the rewrite, without it, without the index use and without the index';

SELECT 'startsWith', count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(msg));
SELECT 'startsWith', count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'startsWith', count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(msg)) SETTINGS use_skip_indexes = 0;
SELECT 'startsWith', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(msg));
SELECT 'startsWith', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'endsWith', count(), sum(id) FROM tab WHERE arrayExists(x -> endsWith(x, 'rged'), tokens(msg));
SELECT 'endsWith', count(), sum(id) FROM tab WHERE arrayExists(x -> endsWith(x, 'rged'), tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'endsWith', count(), sum(id) FROM tab WHERE arrayExists(x -> endsWith(x, 'rged'), tokens(msg)) SETTINGS use_skip_indexes = 0;
SELECT 'endsWith', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> endsWith(x, 'rged'), tokens(msg));
SELECT 'endsWith', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> endsWith(x, 'rged'), tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'position', count(), sum(id) FROM tab WHERE arrayExists(x -> position(x, 'harg') > 0, tokens(msg));
SELECT 'position', count(), sum(id) FROM tab WHERE arrayExists(x -> position(x, 'harg') > 0, tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'position', count(), sum(id) FROM tab WHERE arrayExists(x -> position(x, 'harg') > 0, tokens(msg)) SETTINGS use_skip_indexes = 0;
SELECT 'position', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> position(x, 'harg') > 0, tokens(msg));
SELECT 'position', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> position(x, 'harg') > 0, tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'like', count(), sum(id) FROM tab WHERE arrayExists(x -> x LIKE 'ch%ed', tokens(msg));
SELECT 'like', count(), sum(id) FROM tab WHERE arrayExists(x -> x LIKE 'ch%ed', tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'like', count(), sum(id) FROM tab WHERE arrayExists(x -> x LIKE 'ch%ed', tokens(msg)) SETTINGS use_skip_indexes = 0;
SELECT 'like', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> x LIKE 'ch%ed', tokens(msg));
SELECT 'like', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> x LIKE 'ch%ed', tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'match', count(), sum(id) FROM tab WHERE arrayExists(x -> match(x, '^[0-9]{5}$'), tokens(msg));
SELECT 'match', count(), sum(id) FROM tab WHERE arrayExists(x -> match(x, '^[0-9]{5}$'), tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'match', count(), sum(id) FROM tab WHERE arrayExists(x -> match(x, '^[0-9]{5}$'), tokens(msg)) SETTINGS use_skip_indexes = 0;
SELECT 'match', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> match(x, '^[0-9]{5}$'), tokens(msg));
SELECT 'match', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> match(x, '^[0-9]{5}$'), tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'explicit tokenizer', count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'Charg'), tokens(msg, 'splitByNonAlpha'));
SELECT 'explicit tokenizer', count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'Charg'), tokens(msg, 'splitByNonAlpha')) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'explicit tokenizer', count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'Charg'), tokens(msg, 'splitByNonAlpha')) SETTINGS use_skip_indexes = 0;
SELECT 'explicit tokenizer', count(), sum(id) FROM tab_plain WHERE arrayExists(x -> startsWith(x, 'Charg'), tokens(msg, 'splitByNonAlpha'));

SELECT 'NOT', count(), sum(id) FROM tab WHERE NOT arrayExists(x -> position(x, 'harg') > 0, tokens(msg));
SELECT 'NOT', count(), sum(id) FROM tab_plain WHERE NOT arrayExists(x -> position(x, 'harg') > 0, tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'SELECT list', arrayExists(x -> endsWith(x, 'rged'), tokens(msg)) AS h, count() FROM tab GROUP BY h ORDER BY h;
SELECT 'SELECT list', arrayExists(x -> endsWith(x, 'rged'), tokens(msg)) AS h, count() FROM tab_plain GROUP BY h ORDER BY h SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'empty needle', count() FROM tab WHERE arrayExists(x -> position(x, '') > 0, tokens(msg));
SELECT 'empty needle', count() FROM tab_plain WHERE arrayExists(x -> position(x, '') > 0, tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT '-- the rewrite';

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(msg));
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> endsWith(x, 'rged'), tokens(msg));
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> position(x, 'harg') > 0, tokens(msg));
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> x LIKE 'ch%ed', tokens(msg));
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> match(x, '^[0-9]{5}$'), tokens(msg));
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, 'Charg'), tokens(msg, 'splitByNonAlpha'));
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, '50%_\\'), tokens(msg));

SELECT '-- the index prunes granules and is read directly';

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(msg))) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE arrayExists(x -> position(x, 'harg') > 0, tokens(msg))) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE arrayExists(x -> match(x, '^[0-9]{5}$'), tokens(msg))) WHERE explain LIKE '%Granules:%';
SELECT 'direct read', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION arrayExists(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE arrayExists(x -> x LIKE 'ch%ed', tokens(msg)));

SELECT '-- not rewritten';

-- A needle that depends on a column, or is not constant.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, toString(id)), tokens(msg));
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, materialize('charg')), tokens(msg));
-- A lambda body that references a column.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, 'charg') AND id > 0, tokens(msg));
-- A lambda with two arguments.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists((x, y) -> startsWith(x, 'charg'), tokens(msg), tokens(msg));
-- Tokenizer parameters in separate arguments.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, 'cha'), tokens(msg, 'ngrams', 3));
-- Nullable input: the function would return Nullable(UInt8).
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(toNullable(msg)));
-- Other predicates.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> x ILIKE 'CH%ED', tokens(msg));
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> position(x, 'harg') > 1, tokens(msg));
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith('charged', x), tokens(msg));

SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, toString(id)), tokens(msg));
SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, 'cha'), tokens(msg, 'ngrams', 3));
SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(toNullable(msg)));

SELECT '-- an invalid pattern raises the same exception';
SELECT count() FROM tab WHERE arrayExists(x -> match(x, '('), tokens(msg)); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT count() FROM tab WHERE arrayExists(x -> match(x, '('), tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0; -- { serverError CANNOT_COMPILE_REGEXP }

SELECT '-- a tokenizer other than the index tokenizer does not use the index';

SELECT count() FROM tab WHERE arrayExists(x -> position(x, 'har') > 0, tokens(msg, 'ngrams(3)'));
SELECT count() FROM tab_plain WHERE arrayExists(x -> position(x, 'har') > 0, tokens(msg, 'ngrams(3)')) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE arrayExists(x -> position(x, 'har') > 0, tokens(msg, 'ngrams(3)'))) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT '-- lower preprocessor: the rewritten functions do not apply it, so the result stays case-sensitive';

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(msg)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT id, msg FROM tab_plain;

SELECT count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'Charg'), tokens(msg));
SELECT count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'Charg'), tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'Charg'), tokens(msg)) SETTINGS use_skip_indexes = 0;
SELECT count(), sum(id) FROM tab_plain WHERE arrayExists(x -> startsWith(x, 'Charg'), tokens(msg));
SELECT count(), sum(id) FROM tab WHERE arrayExists(x -> x LIKE 'Charg%', tokens(msg));
SELECT count(), sum(id) FROM tab_plain WHERE arrayExists(x -> x LIKE 'Charg%', tokens(msg)) SETTINGS optimize_rewrite_array_exists_to_has = 0;

DROP TABLE tab;

SELECT '-- an index on an expression serves the rewrite over the same expression';

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(lower(msg)) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT id, msg FROM tab_plain;

SELECT count(), sum(id) FROM tab WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(lower(msg), 'splitByNonAlpha'));
SELECT count(), sum(id) FROM tab_plain WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(lower(msg), 'splitByNonAlpha')) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE arrayExists(x -> startsWith(x, 'charg'), tokens(lower(msg), 'splitByNonAlpha'))) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;
DROP TABLE tab_plain;

SELECT '-- LIKE metacharacters in the needle, FixedString input';

CREATE TABLE tab (id UInt32, msg String, fs FixedString(8)) ENGINE = Memory;
INSERT INTO tab VALUES (1, '50%_off now', '50%'), (2, '50% off xay', '50%_off'), (3, '5000 off a\\b', 'a\\b'), (4, 'x_y 50', '');

SELECT 'startsWith', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> startsWith(x, '50%_'), tokens(msg, 'splitByString([\' \'])'));
SELECT 'startsWith', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> startsWith(x, '50%_'), tokens(msg, 'splitByString([\' \'])')) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'endsWith', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> endsWith(x, '_y'), tokens(msg, 'splitByString([\' \'])'));
SELECT 'endsWith', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> endsWith(x, '_y'), tokens(msg, 'splitByString([\' \'])')) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'position', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> position(x, '\\') > 0, tokens(msg, 'splitByString([\' \'])'));
SELECT 'position', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> position(x, '\\') > 0, tokens(msg, 'splitByString([\' \'])')) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'FixedString', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> endsWith(x, '%'), tokens(fs, 'array'));
SELECT 'FixedString', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> endsWith(x, '%'), tokens(fs, 'array')) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'FixedString', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> startsWith(x, 'a\\'), tokens(fs, 'array'));
SELECT 'FixedString', arraySort(groupArray(id)) FROM tab WHERE arrayExists(x -> startsWith(x, 'a\\'), tokens(fs, 'array')) SETTINGS optimize_rewrite_array_exists_to_has = 0;

DROP TABLE tab;
