-- Tags: no-parallel-replicas
-- `hasTokenPrefix`, `hasTokenLike` and `hasTokenMatch` use the text index: the dictionary tokens matching the
-- needle are exactly the tokens the functions look for, so the index prunes granules and answers them by direct read.

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

-- 128 granules of 8 rows. Only a few granules have tokens starting with 'charg', 'recharge' only contains it.
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
SELECT arraySort(groupArray(id)) FROM tab WHERE hasTokenPrefix(msg, 'charg');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE arrayExists(t -> startsWith(t, 'charg'), tokens(msg));

SELECT arraySort(groupArray(id)) FROM tab WHERE hasTokenLike(msg, '%harg%');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasTokenLike(msg, '%harg%') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE arrayExists(t -> like(t, '%harg%'), tokens(msg));

SELECT arraySort(groupArray(id)) FROM tab WHERE hasTokenMatch(msg, '^[0-9]{5}$');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasTokenMatch(msg, '^[0-9]{5}$') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE arrayExists(t -> match(t, '^[0-9]{5}$'), tokens(msg));

SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'logo');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'nothing');
SELECT count() FROM tab WHERE NOT hasTokenPrefix(msg, 'log');
SELECT count() FROM tab WHERE NOT hasTokenPrefix(msg, 'log') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg') OR hasTokenMatch(msg, '^[0-9]{5}$');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg') OR hasTokenMatch(msg, '^[0-9]{5}$') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenLike(msg, 'log%') AND hasToken(msg, 'logout');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg', 'splitByNonAlpha');

SELECT '-- granules are pruned';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg')) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenLike(msg, '%harg%')) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenMatch(msg, '^[0-9]{5}$')) WHERE explain LIKE '%Granules:%';

SELECT '-- direct read replaces the function';
SELECT 'hasTokenPrefix', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenPrefix(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg'));
SELECT 'hasTokenLike', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenLike(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenLike(msg, '%harg%'));
SELECT 'hasTokenMatch', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenMatch(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenMatch(msg, '^[0-9]{5}$'));

SELECT '-- a different tokenizer does not use the index';
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg', 'ngrams(3)');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'har', 'ngrams(3)');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'har', 'ngrams(3)')) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasTokenLike(msg, 'recharge failed%', 'array');

SELECT '-- invalid patterns raise an exception with the index as well';
SELECT count() FROM tab WHERE hasTokenMatch(msg, '('); -- { serverError CANNOT_COMPILE_REGEXP }

SELECT '-- too many matching posting lists: the functions are evaluated on the column';
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'u') SETTINGS text_index_like_max_postings_to_read = 0, log_comment = 'has_token_pattern_fallback';
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'u') SETTINGS use_skip_indexes = 0;

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

SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE NOT hasTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg')) WHERE explain LIKE '%Granules:%';

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

SELECT count() FROM tab WHERE hasTokenPrefix(arr, 'charg');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(arr, 'charg')) WHERE explain LIKE '%Granules:%';

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

SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'Charg');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'CHARG');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'CHARG')) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasTokenLike(msg, 'charg%');
SELECT count() FROM tab WHERE hasTokenLike(msg, 'Charg%');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenLike(msg, 'Charg%')) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasTokenMatch(msg, '^C');

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

SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'Env:prod');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'Env:prod') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'env:prod');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod');

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

SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'env:prod');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'env:prod', 'array');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod', 'splitByNonAlpha');
SELECT count() FROM tab WHERE hasTokenLike(tag, 'env:%-eu');
SELECT count() FROM tab WHERE hasTokenMatch(tag, '^env:[a-z]+-');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'env:prod')) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod', 'splitByNonAlpha')) WHERE explain LIKE '%Granules:%';
-- Also in the SELECT list.
SELECT hasTokenPrefix(tag, 'env:prod') AS h, count() FROM tab GROUP BY h ORDER BY h;

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

SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'id1') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_prefix';
SELECT count() FROM tab WHERE hasTokenLike(msg, 'id1%') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_token_like';
SELECT count() FROM tab WHERE hasTokenMatch(msg, '^id[0-9]*5$') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_match';
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'id1') SETTINGS text_index_like_max_matched_tokens = 0, log_comment = 'has_token_pattern_matched_tokens_unlimited';
-- LIKE, ILIKE and startsWith are not capped, and their tokens do not count towards the cap of a per-token function.
SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_like';
SELECT count() FROM tab WHERE msg ILIKE '%ID12%' SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_ilike';
SELECT count() FROM tab WHERE startsWith(msg, 'id12') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_starts_with';
SELECT count() FROM tab WHERE msg LIKE '%id12%' OR hasTokenPrefix(msg, 'id199') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = 'has_token_pattern_matched_tokens_like_or_prefix';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS text_index_like_max_matched_tokens = 100) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'id1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenMatch(msg, '^id[0-9]*5$') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE msg LIKE '%id12%' OR hasTokenPrefix(msg, 'id199') SETTINGS use_skip_indexes = 0;

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

SELECT 'hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'env:prod');
SELECT 'hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'env:prod') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'env:prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'env:prod') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'env:prod') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'env:prod') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'prod');
SELECT 'hasTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'prod') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'prod') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'prod') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'prod') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenPrefix(tag, \'prod\')', count() FROM tab_array WHERE hasTokenPrefix(tag, 'prod') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasTokenLike(tag, 'env:%-eu');
SELECT 'hasTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasTokenLike(tag, 'env:%-eu') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasTokenLike(tag, 'env:%-eu') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasTokenLike(tag, 'env:%-eu') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasTokenLike(tag, 'env:%-eu') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasTokenLike(tag, 'env:%-eu') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenLike(tag, \'env:%-eu\')', count() FROM tab_array WHERE hasTokenLike(tag, 'env:%-eu') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenMatch(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasTokenMatch(tag, '^env:[a-z]+-');
SELECT 'hasTokenMatch(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasTokenMatch(tag, '^env:[a-z]+-') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenMatch(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasTokenMatch(tag, '^env:[a-z]+-') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenMatch(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasTokenMatch(tag, '^env:[a-z]+-') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenMatch(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasTokenMatch(tag, '^env:[a-z]+-') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenMatch(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasTokenMatch(tag, '^env:[a-z]+-') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenMatch(tag, \'^env:[a-z]+-\')', count() FROM tab_array WHERE hasTokenMatch(tag, '^env:[a-z]+-') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasTokenPrefix(tag, '');
SELECT 'hasTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasTokenPrefix(tag, '') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasTokenPrefix(tag, '') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasTokenPrefix(tag, '') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasTokenPrefix(tag, '') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasTokenPrefix(tag, '') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenPrefix(tag, \'\')', count() FROM tab_array WHERE hasTokenPrefix(tag, '') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'NOT hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasTokenPrefix(tag, 'env:prod');
SELECT 'NOT hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasTokenPrefix(tag, 'env:prod') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'NOT hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasTokenPrefix(tag, 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'NOT hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasTokenPrefix(tag, 'env:prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'NOT hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasTokenPrefix(tag, 'env:prod') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'NOT hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasTokenPrefix(tag, 'env:prod') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'NOT hasTokenPrefix(tag, \'env:prod\')', count() FROM tab_array WHERE NOT hasTokenPrefix(tag, 'env:prod') SETTINGS text_index_like_max_postings_to_read = 0;

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

SELECT 'hasTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'Charg');
SELECT 'hasTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'Charg') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'Charg') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'Charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'Charg') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'Charg') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenPrefix(msg, \'Charg\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'Charg') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'CHARG');
SELECT 'hasTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'CHARG') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'CHARG') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'CHARG') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'CHARG') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'CHARG') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenPrefix(msg, \'CHARG\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, 'CHARG') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'charg%');
SELECT 'hasTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'charg%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'charg%') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'charg%') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'charg%') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'charg%') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenLike(msg, \'charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'charg%') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'Charg%');
SELECT 'hasTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'Charg%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'Charg%') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'Charg%') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'Charg%') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'Charg%') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenLike(msg, \'Charg%\')', count() FROM tab_lower WHERE hasTokenLike(msg, 'Charg%') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenMatch(msg, \'^C\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^C');
SELECT 'hasTokenMatch(msg, \'^C\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^C') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenMatch(msg, \'^C\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^C') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenMatch(msg, \'^C\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^C') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenMatch(msg, \'^C\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^C') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenMatch(msg, \'^C\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^C') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenMatch(msg, \'^C\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^C') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenMatch(msg, \'^c\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^c');
SELECT 'hasTokenMatch(msg, \'^c\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^c') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenMatch(msg, \'^c\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^c') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenMatch(msg, \'^c\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^c') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenMatch(msg, \'^c\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^c') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenMatch(msg, \'^c\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^c') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenMatch(msg, \'^c\')', count() FROM tab_lower WHERE hasTokenMatch(msg, '^c') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, '');
SELECT 'hasTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, '') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, '') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, '') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, '') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, '') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenPrefix(msg, \'\')', count() FROM tab_lower WHERE hasTokenPrefix(msg, '') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasTokenPrefix(msg, 'charg');
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_lower WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_postings_to_read = 0;
-- Also in the SELECT list.
SELECT countIf(hasTokenPrefix(msg, 'Charg')), countIf(hasTokenLike(msg, 'charg%')) FROM tab_lower;
SELECT countIf(hasTokenPrefix(msg, 'Charg')), countIf(hasTokenLike(msg, 'charg%')) FROM tab_lower SETTINGS use_skip_indexes = 0;

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

SELECT 'hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg');
SELECT 'hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasTokenPrefix(msg, 'charg');
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'NOT hasTokenPrefix(msg, \'charg\')', count() FROM tab_nullable WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'hasTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') IS NULL;
SELECT 'hasTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') IS NULL SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'hasTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') IS NULL SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') IS NULL SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') IS NULL SETTINGS use_skip_indexes_on_data_read = 0;
SELECT 'hasTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') IS NULL SETTINGS text_index_like_max_matched_tokens = 1;
SELECT 'hasTokenPrefix(msg, \'charg\') IS NULL', count() FROM tab_nullable WHERE hasTokenPrefix(msg, 'charg') IS NULL SETTINGS text_index_like_max_postings_to_read = 0;

DROP TABLE tab_nullable;

SELECT '-- several text indexes on one expression must give the function the same tokenizer';

-- A column has at most one text index.
CREATE TABLE tab_two (id UInt32, msg String, INDEX idx_a(msg) TYPE text(tokenizer = splitByNonAlpha), INDEX idx_b(msg) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(msg))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- But differently written expressions can both be the indexed expression: `tag != ''` is analyzed as `notEmpty(tag)`.
-- Different tokenizers: the function throws whatever the settings, unless the tokenizer argument selects one index.
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

SELECT count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS use_skip_indexes = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS query_plan_direct_read_from_text_index = 0; -- { serverError BAD_ARGUMENTS }
SELECT countIf(hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod')) FROM tab_tokenizers; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers WHERE hasTokenLike(if(notEmpty(tag), tag, 'none'), 'env:%'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers WHERE hasTokenMatch(if(notEmpty(tag), tag, 'none'), '^env'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS use_skip_indexes = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab_tokenizers_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS query_plan_direct_read_from_text_index = 0; -- { serverError BAD_ARGUMENTS }
SELECT countIf(hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod')) FROM tab_tokenizers_swapped; -- { serverError BAD_ARGUMENTS }

SELECT 'array', count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'array');
SELECT 'array', count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'array', count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'array') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'array', count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'array');
SELECT 'array', count() FROM tab_tokenizers_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'array');
SELECT 'array', count() FROM tab_tokenizers_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'splitByNonAlpha');
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'splitByNonAlpha') SETTINGS use_skip_indexes = 0;
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'splitByNonAlpha') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'splitByNonAlpha');
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod', 'splitByNonAlpha');
SELECT 'splitByNonAlpha', count() FROM tab_tokenizers_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod', 'splitByNonAlpha') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_tokenizers;
DROP TABLE tab_tokenizers_swapped;

-- Same tokenizer, and only one index has a preprocessor, which the functions never apply: the indexes agree, and the
-- result is the one on the raw values whichever index serves the function.
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

SELECT 'hasTokenPrefix', count() FROM tab_preprocessors WHERE hasTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg');
SELECT 'hasTokenPrefix', count() FROM tab_preprocessors WHERE hasTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix', count() FROM tab_preprocessors WHERE hasTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenPrefix', countIf(hasTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg')) FROM tab_preprocessors;
SELECT 'hasTokenPrefix', count() FROM tab_preprocessors WHERE hasTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg', 'splitByNonAlpha');
SELECT 'hasTokenPrefix', count() FROM tab_preprocessors_swapped WHERE hasTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg');
SELECT 'hasTokenPrefix', count() FROM tab_preprocessors_swapped WHERE hasTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenPrefix', countIf(hasTokenPrefix(if(notEmpty(msg), msg, 'none'), 'Charg')) FROM tab_preprocessors_swapped;

SELECT 'hasTokenLike', count() FROM tab_preprocessors WHERE hasTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%');
SELECT 'hasTokenLike', count() FROM tab_preprocessors WHERE hasTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenLike', count() FROM tab_preprocessors WHERE hasTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasTokenLike', countIf(hasTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%')) FROM tab_preprocessors;
SELECT 'hasTokenLike', count() FROM tab_preprocessors_swapped WHERE hasTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%');
SELECT 'hasTokenLike', count() FROM tab_preprocessors_swapped WHERE hasTokenLike(if(notEmpty(msg), msg, 'none'), 'Charg%') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenMatch', count() FROM tab_preprocessors WHERE hasTokenMatch(if(notEmpty(msg), msg, 'none'), '^C');
SELECT 'hasTokenMatch', count() FROM tab_preprocessors_swapped WHERE hasTokenMatch(if(notEmpty(msg), msg, 'none'), '^C') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_preprocessors;
DROP TABLE tab_preprocessors_swapped;

-- The indexes agree: the function uses their tokenizer whatever the settings, and the first index by name serves it.
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

SELECT 'env:prod', count() FROM tab_agree WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod');
SELECT 'env:prod', count() FROM tab_agree WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'env:prod', count() FROM tab_agree WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'env:prod', countIf(hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod')) FROM tab_agree;
SELECT 'prod', count() FROM tab_agree WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod');
SELECT 'prod', count() FROM tab_agree WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS use_skip_indexes = 0;
SELECT 'prod', count() FROM tab_agree WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'prod', countIf(hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod')) FROM tab_agree;
SELECT 'env:prod', count() FROM tab_agree_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod');
SELECT 'env:prod', count() FROM tab_agree_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'prod', count() FROM tab_agree_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod');
SELECT 'prod', count() FROM tab_agree_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'prod') SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_agree WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod')) WHERE explain LIKE '%Granules:%';
SELECT 'direct read by idx_a', countIf(explain LIKE '%\_\_text\_index\_idx\_a\_hasTokenPrefix%') > 0, countIf(explain LIKE '%\_\_text\_index\_idx\_b\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_agree WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod'));
SELECT 'direct read by idx_a', countIf(explain LIKE '%\_\_text\_index\_idx\_a\_hasTokenPrefix%') > 0, countIf(explain LIKE '%\_\_text\_index\_idx\_b\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_agree_swapped WHERE hasTokenPrefix(if(notEmpty(tag), tag, 'none'), 'env:prod'));

DROP TABLE tab_agree;
DROP TABLE tab_agree_swapped;
