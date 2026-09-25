-- Tags: no-parallel-replicas
-- `hasAnyTokenPrefix`, `hasAnyTokenLike`, `hasAllTokenLike` and `hasAnyTokenRegexp` use the text index to skip granules and to answer by direct read.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

-- 128 granules of 8 rows. Few granules have a token starting with 'charg' ('recharge' does not).
INSERT INTO tab SELECT
    number,
    multiIf(
        number < 8, 'Payment charged twice',
        number >= 400 AND number < 408, 'recharge failed for order 12345',
        number >= 1016, 'Charging station 123456 is busy',
        number % 2 = 0, 'user login ok',
        'user logout ok')
FROM numbers(1024);

SELECT '-- results are the same as without the index and as arrayExists over tokens';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenPrefix(msg, 'charg');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE arrayExists(t -> startsWith(t, 'charg'), tokens(msg));

SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenLike(msg, '%harg%');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenLike(msg, '%harg%') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE arrayExists(t -> like(t, '%harg%'), tokens(msg));

SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenRegexp(msg, '^[0-9]{5}$');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenRegexp(msg, '^[0-9]{5}$') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE arrayExists(t -> match(t, '^[0-9]{5}$'), tokens(msg));

SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'logo');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'nothing');
SELECT count() FROM tab WHERE NOT hasAnyTokenPrefix(msg, 'log');
SELECT count() FROM tab WHERE NOT hasAnyTokenPrefix(msg, 'log') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg') OR hasAnyTokenRegexp(msg, '^[0-9]{5}$');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg') OR hasAnyTokenRegexp(msg, '^[0-9]{5}$') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, 'log%') AND hasToken(msg, 'logout');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg', 'splitByNonAlpha');

SELECT '-- granules are pruned';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg')) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenLike(msg, '%harg%')) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^[0-9]{5}$')) WHERE explain LIKE '%Granules:%';

SELECT '-- direct read replaces the function';
SELECT 'hasAnyTokenPrefix', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAnyTokenPrefix(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg'));
SELECT 'hasAnyTokenLike', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAnyTokenLike(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAnyTokenLike(msg, '%harg%'));
SELECT 'hasAnyTokenRegexp', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAnyTokenRegexp(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^[0-9]{5}$'));

SELECT '-- arrays of patterns';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenPrefix(msg, ['charg', '1234']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenPrefix(msg, ['charg', '1234']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenLike(msg, ['ch%ed', '%ing']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenLike(msg, ['ch%ed', '%ing']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenRegexp(msg, ['^[0-9]{5}$', '^Ch']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenRegexp(msg, ['^[0-9]{5}$', '^Ch']) SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['charg', '1234'])) WHERE explain LIKE '%Granules:%';
SELECT 'hasAnyTokenPrefix', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAnyTokenPrefix(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['charg', '1234']));

SELECT '-- hasAllTokenLike: one pattern is exact, several patterns are a hint';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['charg%']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['%harg%', '1234%']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['%harg%', '1234%']) SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice'])) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']) SETTINGS query_plan_text_index_add_hint = 0) WHERE explain LIKE '%Granules:%';
SELECT 'one pattern', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAllTokenLike(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%']));
SELECT 'two patterns', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAllTokenLike(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']));
SELECT 'two patterns, no hint', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAllTokenLike(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']) SETTINGS query_plan_text_index_add_hint = 0);

SELECT '-- an empty array or an empty pattern does not use the index';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, []);
SELECT count() FROM tab WHERE hasAllTokenLike(msg, []);
SELECT count() FROM tab WHERE NOT hasAllTokenLike(msg, []);
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['', 'zzz']);
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, ['', 'zzz']);
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, ['', 'zzz']);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenLike(msg, ['', 'zzz'])) WHERE explain LIKE '%Granules:%';

SELECT '-- a different tokenizer does not use the index';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg', 'ngrams(3)');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'har', 'ngrams(3)');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'har', 'ngrams(3)')) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, 'recharge failed%', 'array');

SELECT '-- invalid patterns raise an exception with the index as well';
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '('); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, ['^charg', '(']); -- { serverError CANNOT_COMPILE_REGEXP }

SELECT '-- too many matching posting lists: the functions are evaluated on the column';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'u') SETTINGS text_index_like_max_postings_to_read = 0, log_comment = 'has_token_pattern_fallback';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'u') SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['TextIndexDiscardPatternScan'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment = 'has_token_pattern_fallback';

DROP TABLE tab;

SELECT '-- Nullable column: the index prunes granules, NULL rows stay NULL';

CREATE TABLE tab
(
    id UInt32,
    msg Nullable(String),
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, if(number % 3 = 0, NULL, if(number < 8, 'charged', 'other')) FROM numbers(64);

SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE NOT hasAnyTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg')) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT '-- hasAllTokenLike: the patterns match in the same granule, but in different rows';

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, multiIf(number < 4, 'charged twice', number < 8, 'order 12345', 'user login ok') FROM numbers(64);

SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', '12345']);
SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', '12345']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE NOT hasAllTokenLike(msg, ['charg%', '12345']);
SELECT count() FROM tab WHERE NOT hasAllTokenLike(msg, ['charg%', '12345']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'tw%']);
SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'tw%']) SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', '12345'])) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT '-- Array(String) column';

CREATE TABLE tab
(
    id UInt32,
    arr Array(String),
    INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, if(number < 8, ['a b', 'charged'], ['other']) FROM numbers(64);

SELECT count() FROM tab WHERE hasAnyTokenPrefix(arr, 'charg');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(arr, 'charg')) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT '-- lower preprocessor: the functions see the raw values and do not use the index';

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(msg)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, if(number < 8, 'Charged', 'other') FROM numbers(64);

SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'Charg');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'CHARG');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'CHARG')) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, 'charg%');
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, 'Charg%');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenLike(msg, 'Charg%')) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^C');

DROP TABLE tab;

-- The tokenizer of the index is still used.
CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array, preprocessor = lower(tag)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, if(number < 8, 'Env:prod-eu', 'env:dev') FROM numbers(64);

SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'Env:prod');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'Env:prod') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'env:prod');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'prod');

DROP TABLE tab;

SELECT '-- without the tokenizer argument, the tokenizer of the index is used (as for hasAnyTokens)';

CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, if(number < 8, 'env:prod-eu', 'env:dev') FROM numbers(64);

SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'env:prod');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'env:prod', 'array');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'prod');
SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'prod', 'splitByNonAlpha');
SELECT count() FROM tab WHERE hasAnyTokenLike(tag, 'env:%-eu');
SELECT count() FROM tab WHERE hasAnyTokenRegexp(tag, '^env:[a-z]+-');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'env:prod')) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, 'prod', 'splitByNonAlpha')) WHERE explain LIKE '%Granules:%';
-- Also in the SELECT list.
SELECT hasAnyTokenPrefix(tag, 'env:prod') AS h, count() FROM tab GROUP BY h ORDER BY h;

DROP TABLE tab;

SELECT '-- a prefix is matched literally';

CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

-- One value per granule.
INSERT INTO tab SELECT number, ['a.c', 'abc', '50%', '500', 'x_y', 'xzy', 'a\\b', 'ab'][intDiv(number, 8) + 1] FROM numbers(64);

SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, ['a.', '50%', 'x_', 'a\\']);
SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, ['a.', '50%', 'x_', 'a\\']) SETTINGS use_skip_indexes = 0;
SELECT countIf(arrayExists(p -> startsWith(tag, p), ['a.', '50%', 'x_', 'a\\'])) FROM tab;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(tag, ['a.', '50%', 'x_', 'a\\'])) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT '-- text_index_like_max_matched_tokens: a per-token pattern matching too many tokens is evaluated on the column';

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

-- Every row has its own token, so all postings are small and embedded.
INSERT INTO tab SELECT number, concat('req id', toString(number), ' ok') FROM numbers(2000);

SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'id1') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_prefix';
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, 'id1%') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_token_like';
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^id[0-9]*5$') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_match';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'id1') SETTINGS text_index_like_max_matched_tokens = 0, log_comment = 'has_token_pattern_matched_tokens_unlimited';
-- LIKE, ILIKE, startsWith and endsWith are not capped.
-- The endsWith needle is one character, so it matches 200 tokens.
SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_like';
SELECT count() FROM tab WHERE msg ILIKE '%ID12%' SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_ilike';
SELECT count() FROM tab WHERE startsWith(msg, 'id12') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_starts_with';
SELECT count() FROM tab WHERE endsWith(msg, '5') SETTINGS text_index_like_min_pattern_length = 1, text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_ends_with';
SELECT count() FROM tab WHERE msg LIKE '%id12%' OR hasAnyTokenPrefix(msg, 'id199') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_like_or_prefix';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS text_index_like_max_matched_tokens = 100) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE endsWith(msg, '5') SETTINGS text_index_like_min_pattern_length = 1, text_index_like_max_matched_tokens = 100) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'id1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^id[0-9]*5$') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE msg LIKE '%id12%' OR hasAnyTokenPrefix(msg, 'id199') SETTINGS use_skip_indexes = 0;
-- The cap counts distinct tokens of all patterns: 111 tokens start with 'id19', and 111 with 'id18'.
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'id19') SETTINGS text_index_like_max_matched_tokens = 150, log_comment = 'has_token_pattern_matched_tokens_distinct_one';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['id19', 'id18']) SETTINGS text_index_like_max_matched_tokens = 150, log_comment = 'has_token_pattern_matched_tokens_distinct_two';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['id19', 'id19']) SETTINGS text_index_like_max_matched_tokens = 150, log_comment = 'has_token_pattern_matched_tokens_distinct_same';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['id19', 'id18']) SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['TextIndexDiscardPatternScan'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment LIKE 'has_token_pattern_matched_tokens_%'
ORDER BY log_comment;

DROP TABLE tab;

SELECT '-- the result does not depend on settings';

CREATE TABLE tab_array
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab_array SELECT number, if(number < 8, 'env:prod-eu', 'env:dev') FROM numbers(64);

SELECT 'hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'env:prod');
SELECT 'hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'env:prod') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'env:prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'env:prod') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'env:prod') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'env:prod') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'prod');
SELECT 'hasAnyTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'prod') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'prod') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'prod') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'prod') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, 'prod') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasAnyTokenLike(tag, 'env:%-eu');
SELECT 'hasAnyTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasAnyTokenLike(tag, 'env:%-eu') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasAnyTokenLike(tag, 'env:%-eu') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasAnyTokenLike(tag, 'env:%-eu') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasAnyTokenLike(tag, 'env:%-eu') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasAnyTokenLike(tag, 'env:%-eu') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasAnyTokenLike(tag, 'env:%-eu') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenRegexp(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, '^env:[a-z]+-');
SELECT 'hasAnyTokenRegexp(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, '^env:[a-z]+-') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenRegexp(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, '^env:[a-z]+-') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenRegexp(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, '^env:[a-z]+-') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenRegexp(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, '^env:[a-z]+-') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenRegexp(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, '^env:[a-z]+-') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenRegexp(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, '^env:[a-z]+-') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, '');
SELECT 'hasAnyTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, '') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, '') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, '') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, '') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, '') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, '') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'NOT hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasAnyTokenPrefix(tag, 'env:prod');
SELECT 'NOT hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasAnyTokenPrefix(tag, 'env:prod') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'NOT hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasAnyTokenPrefix(tag, 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'NOT hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasAnyTokenPrefix(tag, 'env:prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'NOT hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasAnyTokenPrefix(tag, 'env:prod') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'NOT hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasAnyTokenPrefix(tag, 'env:prod') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'NOT hasAnyTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasAnyTokenPrefix(tag, 'env:prod') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenPrefix(tag, [\'env:prod\', \'x\'])', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, ['env:prod', 'x']);
SELECT 'hasAnyTokenPrefix(tag, [\'env:prod\', \'x\'])', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, ['env:prod', 'x']) SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenPrefix(tag, [\'env:prod\', \'x\'])', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, ['env:prod', 'x']) SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix(tag, [\'env:prod\', \'x\'])', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, ['env:prod', 'x']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix(tag, [\'env:prod\', \'x\'])', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, ['env:prod', 'x']) SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenPrefix(tag, [\'env:prod\', \'x\'])', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, ['env:prod', 'x']) SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenPrefix(tag, [\'env:prod\', \'x\'])', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, ['env:prod', 'x']) SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenPrefix(tag, [\'env:prod\', \'x\'])', count() FROM tab_array WHERE hasAnyTokenPrefix(tag, ['env:prod', 'x']) SETTINGS query_plan_text_index_add_hint = 0;
SELECT 'hasAllTokenLike(tag, [\'env:%\', \'%-eu\'])', count() FROM tab_array WHERE hasAllTokenLike(tag, ['env:%', '%-eu']);
SELECT 'hasAllTokenLike(tag, [\'env:%\', \'%-eu\'])', count() FROM tab_array WHERE hasAllTokenLike(tag, ['env:%', '%-eu']) SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAllTokenLike(tag, [\'env:%\', \'%-eu\'])', count() FROM tab_array WHERE hasAllTokenLike(tag, ['env:%', '%-eu']) SETTINGS use_skip_indexes = 0;
SELECT 'hasAllTokenLike(tag, [\'env:%\', \'%-eu\'])', count() FROM tab_array WHERE hasAllTokenLike(tag, ['env:%', '%-eu']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAllTokenLike(tag, [\'env:%\', \'%-eu\'])', count() FROM tab_array WHERE hasAllTokenLike(tag, ['env:%', '%-eu']) SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAllTokenLike(tag, [\'env:%\', \'%-eu\'])', count() FROM tab_array WHERE hasAllTokenLike(tag, ['env:%', '%-eu']) SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAllTokenLike(tag, [\'env:%\', \'%-eu\'])', count() FROM tab_array WHERE hasAllTokenLike(tag, ['env:%', '%-eu']) SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAllTokenLike(tag, [\'env:%\', \'%-eu\'])', count() FROM tab_array WHERE hasAllTokenLike(tag, ['env:%', '%-eu']) SETTINGS query_plan_text_index_add_hint = 0;
SELECT 'hasAnyTokenRegexp(tag, [\'^env:[a-z]+-\', \'zzz\'])', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, ['^env:[a-z]+-', 'zzz']);
SELECT 'hasAnyTokenRegexp(tag, [\'^env:[a-z]+-\', \'zzz\'])', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, ['^env:[a-z]+-', 'zzz']) SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenRegexp(tag, [\'^env:[a-z]+-\', \'zzz\'])', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, ['^env:[a-z]+-', 'zzz']) SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenRegexp(tag, [\'^env:[a-z]+-\', \'zzz\'])', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, ['^env:[a-z]+-', 'zzz']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenRegexp(tag, [\'^env:[a-z]+-\', \'zzz\'])', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, ['^env:[a-z]+-', 'zzz']) SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenRegexp(tag, [\'^env:[a-z]+-\', \'zzz\'])', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, ['^env:[a-z]+-', 'zzz']) SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenRegexp(tag, [\'^env:[a-z]+-\', \'zzz\'])', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, ['^env:[a-z]+-', 'zzz']) SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenRegexp(tag, [\'^env:[a-z]+-\', \'zzz\'])', count() FROM tab_array WHERE hasAnyTokenRegexp(tag, ['^env:[a-z]+-', 'zzz']) SETTINGS query_plan_text_index_add_hint = 0;

DROP TABLE tab_array;

CREATE TABLE tab_lower
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(msg)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab_lower SELECT number, if(number < 8, 'Charged', 'other') FROM numbers(64);

SELECT 'hasAnyTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'Charg');
SELECT 'hasAnyTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'Charg') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'Charg') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'Charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'Charg') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'Charg') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'Charg') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'CHARG');
SELECT 'hasAnyTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'CHARG') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'CHARG') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'CHARG') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'CHARG') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'CHARG') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, 'CHARG') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'charg%');
SELECT 'hasAnyTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'charg%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'charg%') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'charg%') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'charg%') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'charg%') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'charg%') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'Charg%');
SELECT 'hasAnyTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'Charg%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'Charg%') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'Charg%') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'Charg%') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'Charg%') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasAnyTokenLike(msg, 'Charg%') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^C\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^C');
SELECT 'hasAnyTokenRegexp(msg, \'^C\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^C') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^C\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^C') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^C\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^C') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^C\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^C') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^C\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^C') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenRegexp(msg, \'^C\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^C') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^c\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^c');
SELECT 'hasAnyTokenRegexp(msg, \'^c\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^c') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^c\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^c') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^c\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^c') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^c\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^c') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenRegexp(msg, \'^c\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^c') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenRegexp(msg, \'^c\')', count() FROM tab_lower WHERE hasAnyTokenRegexp(msg, '^c') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, '');
SELECT 'hasAnyTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, '') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, '') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, '') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, '') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, '') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasAnyTokenPrefix(msg, '') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasAnyTokenPrefix(msg, 'charg');
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_postings_to_read = 0;
-- Also in the SELECT list.
SELECT countIf(hasAnyTokenPrefix(msg, 'Charg')), countIf(hasAnyTokenLike(msg, 'charg%')) FROM tab_lower;
SELECT countIf(hasAnyTokenPrefix(msg, 'Charg')), countIf(hasAnyTokenLike(msg, 'charg%')) FROM tab_lower SETTINGS use_skip_indexes = 0;

DROP TABLE tab_lower;

CREATE TABLE tab_nullable
(
    id UInt32,
    msg Nullable(String),
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab_nullable SELECT number, if(number % 3 = 0, NULL, if(number < 8, 'charged', 'other')) FROM numbers(64);

SELECT 'hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg');
SELECT 'hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasAnyTokenPrefix(msg, 'charg');
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'NOT hasAnyTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasAnyTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAnyTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') IS NULL;
SELECT 'hasAnyTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') IS NULL SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAnyTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') IS NULL SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') IS NULL SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') IS NULL SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAnyTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') IS NULL SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAnyTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasAnyTokenPrefix(msg, 'charg') IS NULL SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE hasAllTokenLike(msg, ['charg%', '%ed']);
SELECT 'hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS use_skip_indexes = 0;
SELECT 'hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'NOT hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE NOT hasAllTokenLike(msg, ['charg%', '%ed']);
SELECT 'NOT hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE NOT hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'NOT hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE NOT hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS use_skip_indexes = 0;
SELECT 'NOT hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE NOT hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'NOT hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE NOT hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'NOT hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE NOT hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'NOT hasAllTokenLike(msg, [\'charg%\', \'%ed\'])', count() FROM tab_nullable WHERE NOT hasAllTokenLike(msg, ['charg%', '%ed']) SETTINGS text_index_like_max_postings_to_read = 0;

DROP TABLE tab_nullable;

SELECT '-- several text indexes on one expression must give the function the same tokenizer';

-- A column has at most one text index.
CREATE TABLE tab_two (id UInt32, msg String, INDEX idx_a(msg) TYPE text(tokenizer = splitByNonAlpha), INDEX idx_b(msg) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(msg))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- Two differently written expressions can be the same indexed expression (`tag != ''` is `notEmpty(tag)`).
-- With different tokenizers the function throws, unless the tokenizer argument picks one index.
CREATE TABLE tab_tokenizers
(
    id UInt32,
    tag String,
    INDEX idx_a(if(tag != '', tag, 'none')) TYPE text(tokenizer = array) GRANULARITY 1,
    INDEX idx_b(if(notEmpty(tag), tag, 'none')) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

CREATE TABLE tab_tokenizers_swapped
(
    id UInt32,
    tag String,
    INDEX idx_b(if(tag != '', tag, 'none')) TYPE text(tokenizer = array) GRANULARITY 1,
    INDEX idx_a(if(notEmpty(tag), tag, 'none')) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab_tokenizers SELECT number, if(number < 8, 'env:prod-eu', 'env:dev') FROM numbers(64);
INSERT INTO tab_tokenizers_swapped SELECT * FROM tab_tokenizers;

SELECT count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS use_skip_indexes = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS query_plan_direct_read_from_text_index = 0; -- { serverError BAD_ARGUMENTS }
SELECT countIf(hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod')) FROM tab_tokenizers; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers WHERE hasAnyTokenLike(if(notEmpty(tag), tag, 'none'), 'env:%'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers WHERE hasAnyTokenRegexp(if(notEmpty(tag), tag, 'none'), '^env'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers WHERE hasAllTokenLike(if(notEmpty(tag), tag, 'none'), ['env:%', '%eu']); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS use_skip_indexes = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS query_plan_direct_read_from_text_index = 0; -- { serverError BAD_ARGUMENTS }
SELECT countIf(hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod')) FROM tab_tokenizers_swapped; -- { serverError BAD_ARGUMENTS }

SELECT 'array', count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'array');
SELECT 'array', count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'array', count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'array') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'array', count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'array');
SELECT 'array', count() FROM tab_tokenizers_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'array');
SELECT 'array', count() FROM tab_tokenizers_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'splitByNonAlpha');
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'splitByNonAlpha') SETTINGS use_skip_indexes = 0;
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'splitByNonAlpha') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'splitByNonAlpha');
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'splitByNonAlpha');
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'splitByNonAlpha') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_tokenizers;
DROP TABLE tab_tokenizers_swapped;

-- Same tokenizer, one index has a preprocessor: the functions ignore it and give the result on the raw values.
-- The index without the preprocessor answers by direct read.
CREATE TABLE tab_preprocessors
(
    id UInt32,
    msg String,
    INDEX idx_a(if(msg != '', msg, 'none')) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1,
    INDEX idx_b(if(notEmpty(msg), msg, 'none')) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(if(notEmpty(msg), msg, 'none'))) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

CREATE TABLE tab_preprocessors_swapped
(
    id UInt32,
    msg String,
    INDEX idx_b(if(msg != '', msg, 'none')) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1,
    INDEX idx_a(if(notEmpty(msg), msg, 'none')) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(if(notEmpty(msg), msg, 'none'))) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab_preprocessors SELECT number, multiIf(number < 8, 'Charged', number < 16, 'charged', 'other') FROM numbers(64);
INSERT INTO tab_preprocessors_swapped SELECT * FROM tab_preprocessors;

SELECT 'hasAnyTokenPrefix', count() FROM tab_preprocessors WHERE hasAnyTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg');
SELECT 'hasAnyTokenPrefix', count() FROM tab_preprocessors WHERE hasAnyTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix', count() FROM tab_preprocessors WHERE hasAnyTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenPrefix', countIf(hasAnyTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg')) FROM tab_preprocessors;
SELECT 'hasAnyTokenPrefix', count() FROM tab_preprocessors WHERE hasAnyTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg', 'splitByNonAlpha');
SELECT 'hasAnyTokenPrefix', count() FROM tab_preprocessors_swapped WHERE hasAnyTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg');
SELECT 'hasAnyTokenPrefix', count() FROM tab_preprocessors_swapped WHERE hasAnyTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenPrefix', countIf(hasAnyTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg')) FROM tab_preprocessors_swapped;
SELECT 'hasAnyTokenPrefix', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAnyTokenPrefix(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_preprocessors_swapped WHERE hasAnyTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg'));

SELECT 'hasAnyTokenLike', count() FROM tab_preprocessors WHERE hasAnyTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%');
SELECT 'hasAnyTokenLike', count() FROM tab_preprocessors WHERE hasAnyTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenLike', count() FROM tab_preprocessors WHERE hasAnyTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasAnyTokenLike', countIf(hasAnyTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%')) FROM tab_preprocessors;
SELECT 'hasAnyTokenLike', count() FROM tab_preprocessors_swapped WHERE hasAnyTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%');
SELECT 'hasAnyTokenLike', count() FROM tab_preprocessors_swapped WHERE hasAnyTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokenRegexp', count() FROM tab_preprocessors WHERE hasAnyTokenRegexp(if(notEmpty(msg), msg, 'none'), '^C');
SELECT 'hasAnyTokenRegexp', count() FROM tab_preprocessors_swapped WHERE hasAnyTokenRegexp(if(notEmpty(msg), msg, 'none'), '^C') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_preprocessors;
DROP TABLE tab_preprocessors_swapped;

-- The indexes agree, so the first one by name serves the function.
CREATE TABLE tab_agree
(
    id UInt32,
    tag String,
    INDEX idx_a(if(tag != '', tag, 'none')) TYPE text(tokenizer = array) GRANULARITY 1,
    INDEX idx_b(if(notEmpty(tag), tag, 'none')) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

CREATE TABLE tab_agree_swapped
(
    id UInt32,
    tag String,
    INDEX idx_b(if(tag != '', tag, 'none')) TYPE text(tokenizer = array) GRANULARITY 1,
    INDEX idx_a(if(notEmpty(tag), tag, 'none')) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab_agree SELECT number, if(number < 8, 'env:prod-eu', 'env:dev') FROM numbers(64);
INSERT INTO tab_agree_swapped SELECT * FROM tab_agree;

SELECT 'env:prod', count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod');
SELECT 'env:prod', count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'env:prod', count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'env:prod', countIf(hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod')) FROM tab_agree;
SELECT 'prod', count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod');
SELECT 'prod', count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS use_skip_indexes = 0;
SELECT 'prod', count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'prod', countIf(hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod')) FROM tab_agree;
SELECT 'env:prod', count() FROM tab_agree_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod');
SELECT 'env:prod', count() FROM tab_agree_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'prod', count() FROM tab_agree_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod');
SELECT 'prod', count() FROM tab_agree_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS use_skip_indexes = 0;
SELECT 'env:prod array', count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), ['env:prod', 'zzz']);
SELECT 'env:prod array', count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), ['env:prod', 'zzz']) SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod')) WHERE explain LIKE '%Granules:%';
SELECT 'direct read by idx_a', countIf(explain LIKE '%\_\_text\_index\_idx\_a\_hasAnyTokenPrefix%') > 0, countIf(explain LIKE '%\_\_text\_index\_idx\_b\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_agree WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod'));
SELECT 'direct read by idx_a', countIf(explain LIKE '%\_\_text\_index\_idx\_a\_hasAnyTokenPrefix%') > 0, countIf(explain LIKE '%\_\_text\_index\_idx\_b\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_agree_swapped WHERE hasAnyTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod'));

DROP TABLE tab_agree;
DROP TABLE tab_agree_swapped;
