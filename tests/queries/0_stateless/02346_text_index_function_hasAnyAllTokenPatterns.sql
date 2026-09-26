-- Tags: no-parallel-replicas
-- `hasAnyTokenPrefix`, `hasAnyTokenLike`, `hasAllTokenLike` and `hasAnyTokenRegexp` use the text index to skip granules and to answer by direct read.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;
SET query_plan_text_index_add_hint = 1;

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
FROM (EXPLAIN actions = 1, compact = 0 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'charg'));
SELECT 'hasAnyTokenLike', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAnyTokenLike(%') > 0
FROM (EXPLAIN actions = 1, compact = 0 SELECT count() FROM tab WHERE hasAnyTokenLike(msg, '%harg%'));
SELECT 'hasAnyTokenRegexp', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAnyTokenRegexp(%') > 0
FROM (EXPLAIN actions = 1, compact = 0 SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^[0-9]{5}$'));

SELECT '-- arrays of patterns';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenPrefix(msg, ['charg', '1234']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenPrefix(msg, ['charg', '1234']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenLike(msg, ['ch%ed', '%ing']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenLike(msg, ['ch%ed', '%ing']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenRegexp(msg, ['^[0-9]{5}$', '^Ch']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokenRegexp(msg, ['^[0-9]{5}$', '^Ch']) SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['charg', '1234'])) WHERE explain LIKE '%Granules:%';
SELECT 'hasAnyTokenPrefix', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAnyTokenPrefix(%') > 0
FROM (EXPLAIN actions = 1, compact = 0 SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['charg', '1234']));

SELECT '-- hasAllTokenLike: one pattern is exact, several patterns are a hint';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['charg%']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['%harg%', '1234%']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokenLike(msg, ['%harg%', '1234%']) SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice'])) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']) SETTINGS query_plan_text_index_add_hint = 0) WHERE explain LIKE '%Granules:%';
SELECT 'one pattern', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAllTokenLike(%') > 0
FROM (EXPLAIN actions = 1, compact = 0 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%']));
SELECT 'two patterns', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAllTokenLike(%') > 0
FROM (EXPLAIN actions = 1, compact = 0 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']));
SELECT 'two patterns, no hint', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasAllTokenLike(%') > 0
FROM (EXPLAIN actions = 1, compact = 0 SELECT count() FROM tab WHERE hasAllTokenLike(msg, ['charg%', 'twice']) SETTINGS query_plan_text_index_add_hint = 0);

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
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'u') SETTINGS text_index_like_max_postings_to_read = 0, log_comment = 'has_any_all_token_patterns_fallback';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'u') SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['TextIndexDiscardPatternScan'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment = 'has_any_all_token_patterns_fallback';

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
-- Half the rows have no id token, so the dictionary is small enough to be scanned as a whole (see the next section).
INSERT INTO tab SELECT number, if(number < 2000, concat('req id', toString(number), ' ok'), 'req ok') FROM numbers(4000);

SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'id1') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_matched_tokens_prefix';
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, 'id1%') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_matched_tokens_token_like';
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^id[0-9]*5$') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_matched_tokens_match';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'id1') SETTINGS text_index_like_max_matched_tokens = 0, log_comment = 'has_any_all_token_patterns_matched_tokens_unlimited';
-- LIKE, ILIKE, startsWith and endsWith are not capped.
-- The endsWith needle is one character, so it matches 200 tokens.
SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_matched_tokens_like';
SELECT count() FROM tab WHERE msg ILIKE '%ID12%' SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_matched_tokens_ilike';
SELECT count() FROM tab WHERE startsWith(msg, 'id12') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_matched_tokens_starts_with';
SELECT count() FROM tab WHERE endsWith(msg, '5') SETTINGS text_index_like_min_pattern_length = 1, text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_matched_tokens_ends_with';
SELECT count() FROM tab WHERE msg LIKE '%id12%' OR hasAnyTokenPrefix(msg, 'id199') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_matched_tokens_like_or_prefix';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS text_index_like_max_matched_tokens = 100) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE endsWith(msg, '5') SETTINGS text_index_like_min_pattern_length = 1, text_index_like_max_matched_tokens = 100) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'id1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^id[0-9]*5$') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE msg LIKE '%id12%' OR hasAnyTokenPrefix(msg, 'id199') SETTINGS use_skip_indexes = 0;
-- The cap counts distinct tokens of all patterns: 111 tokens start with 'id19', and 111 with 'id18'.
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'id19') SETTINGS text_index_like_max_matched_tokens = 150, log_comment = 'has_any_all_token_patterns_matched_tokens_distinct_one';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['id19', 'id18']) SETTINGS text_index_like_max_matched_tokens = 150, log_comment = 'has_any_all_token_patterns_matched_tokens_distinct_two';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['id19', 'id19']) SETTINGS text_index_like_max_matched_tokens = 150, log_comment = 'has_any_all_token_patterns_matched_tokens_distinct_same';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, ['id19', 'id18']) SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['TextIndexDiscardPatternScan'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment LIKE 'has_any_all_token_patterns_matched_tokens_%'
ORDER BY log_comment;

DROP TABLE tab;

SELECT '-- a scan of the whole dictionary is skipped if it holds more tokens than the cap and half the rows';

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 128)
)
ENGINE = MergeTree
ORDER BY id;

-- 6000 tokens in one part of 3000 rows. '%2999%' matches 2 tokens, and 'a299' seeks to 11.
INSERT INTO tab SELECT number, concat('a', toString(number), ' b', toString(number)) FROM numbers(3000);
OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE hasAnyTokenLike(msg, '%2999%') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_scan_size_whole';
SELECT count() FROM tab WHERE hasAnyTokenPrefix(msg, 'a299') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_any_all_token_patterns_scan_size_seek';
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, '%2999%') SETTINGS log_comment = 'has_any_all_token_patterns_scan_size_default';
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, '%2999%') SETTINGS text_index_like_max_matched_tokens = 0, log_comment = 'has_any_all_token_patterns_scan_size_unlimited';
SELECT count() FROM tab WHERE hasAnyTokenLike(msg, '%2999%') SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['TextIndexDiscardPatternScan'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment LIKE 'has_any_all_token_patterns_scan_size_%'
ORDER BY log_comment;

DROP TABLE tab;

SELECT '-- regular expressions give the same result with and without the index, also on invalid UTF-8';

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, multiIf(number % 100 = 7, 'b a\xFF', number % 3 = 0, concat('b a', char(113 + number % 5)), 'b') FROM numbers(1000);

SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^a[^b]+$');
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^a[^b]+$') SETTINGS text_index_like_max_matched_tokens = 2, min_count_to_compile_regular_expression = 0;
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^a[^b]+$') SETTINGS use_skip_indexes = 0, min_count_to_compile_regular_expression = 0;
SELECT count() FROM tab WHERE hasAnyTokenRegexp(msg, '^a[^b]+$') SETTINGS use_skip_indexes = 0, compile_regular_expressions = 0;

DROP TABLE tab;
