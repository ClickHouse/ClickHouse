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

SELECT '-- the index with a preprocessor is not used';

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
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'Charg')) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT '-- the index with an array tokenizer is used with the tokenizer argument only';

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

SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'env:prod', 'array');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'env:prod', 'array')) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod')) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;
