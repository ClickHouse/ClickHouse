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
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'u') SETTINGS text_index_like_max_postings_to_read = 0, log_comment = '05244_fallback';
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'u') SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['TextIndexDiscardPatternScan'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment = '05244_fallback';

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

SELECT '-- lower preprocessor: applied to the input and the prefix of hasTokenPrefix, hasTokenLike and hasTokenMatch do not use the index';

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

SELECT '-- text_index_like_max_matched_tokens: a pattern matching too many tokens is evaluated on the column';

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

SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'id1') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = '05244_matched_tokens_prefix';
SELECT count() FROM tab WHERE hasTokenMatch(msg, '^id[0-9]*5$') SETTINGS text_index_like_max_matched_tokens = 100, log_comment = '05244_matched_tokens_match';
SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS text_index_like_max_matched_tokens = 100, log_comment = '05244_matched_tokens_like';
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'id1') SETTINGS text_index_like_max_matched_tokens = 0, log_comment = '05244_matched_tokens_unlimited';
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'id1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenMatch(msg, '^id[0-9]*5$') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE msg LIKE '%id12%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['TextIndexDiscardPatternScan'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment LIKE '05244_matched_tokens_%'
ORDER BY log_comment;

DROP TABLE tab;

SELECT '-- the result does not depend on settings, only on the index definition';

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
