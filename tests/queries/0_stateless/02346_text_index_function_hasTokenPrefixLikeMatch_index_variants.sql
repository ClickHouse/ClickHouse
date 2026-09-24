-- Tags: no-parallel-replicas
-- `hasTokenPrefix`, `hasTokenLike` and `hasTokenMatch` give the same result with and without the text index for a postprocessor,
-- a map element or JSON path, a LowCardinality column and tokenizers other than `splitByNonAlpha` and `array`.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

SELECT '-- postprocessor: the index is not used and the functions see the tokens without the postprocessor';

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(msg)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, multiIf(number < 8, 'Charged', number < 16, 'charged', 'other') FROM numbers(64);

SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'Charg');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'Charg') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenLike(msg, 'Charg%');
SELECT count() FROM tab WHERE hasTokenLike(msg, 'Charg%') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenMatch(msg, '^C');
SELECT count() FROM tab WHERE hasTokenMatch(msg, '^C') SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'Charg')) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;

-- The tokenizer of the index still applies.
CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array, postprocessor = lower(tag)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, multiIf(number < 8, 'env:Prod-eu', number < 16, 'env:prod-eu', 'env:dev') FROM numbers(64);

SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'env:prod');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod', 'splitByNonAlpha');

DROP TABLE tab;

SELECT '-- map element: an index on mapKeys(m) prunes by the key, an index on mapValues(m) is not used, the functions keep splitByNonAlpha';

CREATE TABLE tab
(
    id UInt32,
    m Map(String, String),
    INDEX idx_keys(mapKeys(m)) TYPE text(tokenizer = array) GRANULARITY 1,
    INDEX idx_values(mapValues(m)) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, multiIf(number < 8, map('k', 'env-prod'), number < 16, map('k', 'production'), number < 24, map('x', 'prod'), map('k', 'dev')) FROM numbers(64);

SELECT count() FROM tab WHERE hasTokenPrefix(m['k'], 'pro');
SELECT count() FROM tab WHERE hasTokenPrefix(m['k'], 'pro') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(m['k'], 'pro') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(m['k'], 'pro', 'array');
SELECT count() FROM tab WHERE hasTokenPrefix(m['k'], 'pro', 'array') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenLike(m['k'], 'pro%');
SELECT count() FROM tab WHERE hasTokenLike(m['k'], 'pro%') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenMatch(m['k'], '^pro');
SELECT count() FROM tab WHERE hasTokenMatch(m['k'], '^pro') SETTINGS use_skip_indexes = 0;
SELECT countIf(hasTokenPrefix(m['k'], 'pro')) FROM tab;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(m['k'], 'pro')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

DROP TABLE tab;

SELECT '-- JSON path: an index on JSONAllPaths(j) prunes by the path, an index on JSONAllValues(j) is not used, the functions keep splitByNonAlpha';

CREATE TABLE tab
(
    id UInt32,
    j JSON,
    INDEX idx_paths(JSONAllPaths(j)) TYPE text(tokenizer = array) GRANULARITY 1,
    INDEX idx_values(JSONAllValues(j)) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, multiIf(number < 8, '{"k": "env-prod"}', number < 16, '{"k": "production"}', number < 24, '{"x": "prod"}', '{"k": "dev"}') FROM numbers(64);

SELECT count() FROM tab WHERE hasTokenPrefix(j.k::String, 'pro');
SELECT count() FROM tab WHERE hasTokenPrefix(j.k::String, 'pro') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(j.k::String, 'pro') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(j.k::String, 'pro', 'array');
SELECT count() FROM tab WHERE hasTokenPrefix(j.k::String, 'pro', 'array') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenLike(j.k::String, 'pro%');
SELECT count() FROM tab WHERE hasTokenMatch(j.k::String, '^pro');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(j.k::String, 'pro')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

DROP TABLE tab;

SELECT '-- LowCardinality(String): direct read';

CREATE TABLE tab
(
    id UInt32,
    msg LowCardinality(String),
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, multiIf(number < 8, 'Payment charged twice', number < 16, 'recharge failed for order 12345', number % 2 = 0, 'user login ok', 'user logout ok') FROM numbers(64);

SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE NOT hasTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE NOT hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenLike(msg, '%harg%');
SELECT count() FROM tab WHERE hasTokenLike(msg, '%harg%') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenMatch(msg, '^[0-9]{5}$');
SELECT count() FROM tab WHERE hasTokenMatch(msg, '^[0-9]{5}$') SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg')) WHERE explain LIKE '%Granules:%';
SELECT 'hasTokenPrefix', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenPrefix(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg'));
SELECT 'hasTokenLike', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenLike(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenLike(msg, '%harg%'));
SELECT 'hasTokenMatch', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenMatch(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenMatch(msg, '^[0-9]{5}$'));

DROP TABLE tab;

SELECT '-- ngrams, sparseGrams, splitByString and asciiCJK: the functions use the tokenizer of the index, as arrayExists over tokens';

CREATE TABLE tab
(
    id UInt32,
    s_ngrams String,
    s_sparse String,
    s_split String,
    s_cjk String,
    INDEX idx_ngrams(s_ngrams) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX idx_sparse(s_sparse) TYPE text(tokenizer = sparseGrams(3, 100)) GRANULARITY 1,
    INDEX idx_split(s_split) TYPE text(tokenizer = splitByString([', ', ' '])) GRANULARITY 1,
    INDEX idx_cjk(s_cjk) TYPE text(tokenizer = asciiCJK) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, s, s, s, s
FROM
(
    SELECT number, multiIf(
        number < 8, 'Payment charged twice, order 12345',
        number < 16, 'recharge failed; order 123456',
        number < 24, 'Charging station 99 is busy',
        number < 32, '支付失败 charge x:y',
        number % 2 = 0, 'user login ok',
        'user logout ok') AS s
    FROM numbers(64)
);

SELECT 'ngrams', count() FROM tab WHERE hasTokenPrefix(s_ngrams, 'har');
SELECT 'ngrams', count() FROM tab WHERE hasTokenPrefix(s_ngrams, 'har') SETTINGS use_skip_indexes = 0;
SELECT 'ngrams', count() FROM tab WHERE hasTokenPrefix(s_ngrams, 'har') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'ngrams', count() FROM tab WHERE hasTokenPrefix(s_ngrams, 'charg');
SELECT 'ngrams', count() FROM tab WHERE hasTokenPrefix(s_ngrams, 'charg') SETTINGS use_skip_indexes = 0;
SELECT 'ngrams', count() FROM tab WHERE hasTokenLike(s_ngrams, '_ar');
SELECT 'ngrams', count() FROM tab WHERE hasTokenLike(s_ngrams, '_ar') SETTINGS use_skip_indexes = 0;
SELECT 'ngrams', count() FROM tab WHERE hasTokenLike(s_ngrams, '_ar') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'ngrams', count() FROM tab WHERE hasTokenMatch(s_ngrams, '^[0-9]{3}$');
SELECT 'ngrams', count() FROM tab WHERE hasTokenMatch(s_ngrams, '^[0-9]{3}$') SETTINGS use_skip_indexes = 0;
SELECT 'ngrams', count() FROM tab WHERE hasTokenMatch(s_ngrams, '^[0-9]{3}$') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'ngrams', countIf(arrayExists(t -> startsWith(t, 'har'), tokens(s_ngrams, 'ngrams(3)'))), countIf(arrayExists(t -> startsWith(t, 'charg'), tokens(s_ngrams, 'ngrams(3)'))),
    countIf(arrayExists(t -> like(t, '_ar'), tokens(s_ngrams, 'ngrams(3)'))), countIf(arrayExists(t -> match(t, '^[0-9]{3}$'), tokens(s_ngrams, 'ngrams(3)'))) FROM tab;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(s_ngrams, 'har')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenLike(s_ngrams, '_ar')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenMatch(s_ngrams, '^[0-9]{3}$')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

SELECT 'sparseGrams', count() FROM tab WHERE hasTokenPrefix(s_sparse, 'ged');
SELECT 'sparseGrams', count() FROM tab WHERE hasTokenPrefix(s_sparse, 'ged') SETTINGS use_skip_indexes = 0;
SELECT 'sparseGrams', count() FROM tab WHERE hasTokenPrefix(s_sparse, 'ged') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'sparseGrams', count() FROM tab WHERE hasTokenLike(s_sparse, '%harg%');
SELECT 'sparseGrams', count() FROM tab WHERE hasTokenLike(s_sparse, '%harg%') SETTINGS use_skip_indexes = 0;
SELECT 'sparseGrams', count() FROM tab WHERE hasTokenLike(s_sparse, '%harg%') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'sparseGrams', count() FROM tab WHERE hasTokenMatch(s_sparse, '^t c');
SELECT 'sparseGrams', count() FROM tab WHERE hasTokenMatch(s_sparse, '^t c') SETTINGS use_skip_indexes = 0;
SELECT 'sparseGrams', count() FROM tab WHERE hasTokenMatch(s_sparse, '^t c') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'sparseGrams', countIf(arrayExists(t -> startsWith(t, 'ged'), tokens(s_sparse, 'sparseGrams(3, 100)'))),
    countIf(arrayExists(t -> like(t, '%harg%'), tokens(s_sparse, 'sparseGrams(3, 100)'))), countIf(arrayExists(t -> match(t, '^t c'), tokens(s_sparse, 'sparseGrams(3, 100)'))) FROM tab;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(s_sparse, 'ged')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenLike(s_sparse, '%harg%')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenMatch(s_sparse, '^t c')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

SELECT 'splitByString', count() FROM tab WHERE hasTokenPrefix(s_split, 'x:');
SELECT 'splitByString', count() FROM tab WHERE hasTokenPrefix(s_split, 'x:') SETTINGS use_skip_indexes = 0;
SELECT 'splitByString', count() FROM tab WHERE hasTokenPrefix(s_split, 'x:') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'splitByString', count() FROM tab WHERE hasTokenLike(s_split, '%;');
SELECT 'splitByString', count() FROM tab WHERE hasTokenLike(s_split, '%;') SETTINGS use_skip_indexes = 0;
SELECT 'splitByString', count() FROM tab WHERE hasTokenLike(s_split, '%;') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'splitByString', count() FROM tab WHERE hasTokenMatch(s_split, '^[a-z]+;$');
SELECT 'splitByString', count() FROM tab WHERE hasTokenMatch(s_split, '^[a-z]+;$') SETTINGS use_skip_indexes = 0;
SELECT 'splitByString', count() FROM tab WHERE hasTokenMatch(s_split, '^[a-z]+;$') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'splitByString', countIf(arrayExists(t -> startsWith(t, 'x:'), tokens(s_split, 'splitByString([\', \', \' \'])'))),
    countIf(arrayExists(t -> like(t, '%;'), tokens(s_split, 'splitByString([\', \', \' \'])'))), countIf(arrayExists(t -> match(t, '^[a-z]+;$'), tokens(s_split, 'splitByString([\', \', \' \'])'))) FROM tab;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(s_split, 'x:')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenLike(s_split, '%;')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenMatch(s_split, '^[a-z]+;$')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

SELECT 'asciiCJK', count() FROM tab WHERE hasTokenPrefix(s_cjk, 'x:');
SELECT 'asciiCJK', count() FROM tab WHERE hasTokenPrefix(s_cjk, 'x:') SETTINGS use_skip_indexes = 0;
SELECT 'asciiCJK', count() FROM tab WHERE hasTokenPrefix(s_cjk, 'x:') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'asciiCJK', count() FROM tab WHERE hasTokenLike(s_cjk, '支');
SELECT 'asciiCJK', count() FROM tab WHERE hasTokenLike(s_cjk, '支') SETTINGS use_skip_indexes = 0;
SELECT 'asciiCJK', count() FROM tab WHERE hasTokenLike(s_cjk, '支') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'asciiCJK', count() FROM tab WHERE hasTokenMatch(s_cjk, '^.:.$');
SELECT 'asciiCJK', count() FROM tab WHERE hasTokenMatch(s_cjk, '^.:.$') SETTINGS use_skip_indexes = 0;
SELECT 'asciiCJK', count() FROM tab WHERE hasTokenMatch(s_cjk, '^.:.$') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'asciiCJK', countIf(arrayExists(t -> startsWith(t, 'x:'), tokens(s_cjk, 'asciiCJK'))),
    countIf(arrayExists(t -> like(t, '支'), tokens(s_cjk, 'asciiCJK'))), countIf(arrayExists(t -> match(t, '^.:.$'), tokens(s_cjk, 'asciiCJK'))) FROM tab;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(s_cjk, 'x:')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenLike(s_cjk, '支')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenMatch(s_cjk, '^.:.$')) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

SELECT 'direct read', countIf(explain LIKE '%\_\_text\_index\_idx\_ngrams\_%') > 0, countIf(explain LIKE '%\_\_text\_index\_idx\_sparse\_%') > 0,
    countIf(explain LIKE '%\_\_text\_index\_idx\_split\_%') > 0, countIf(explain LIKE '%\_\_text\_index\_idx\_cjk\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenPrefix(s_ngrams, 'har') AND hasTokenLike(s_sparse, '%harg%') AND hasTokenMatch(s_split, '^[a-z]+;$') AND hasTokenPrefix(s_cjk, 'x:'));

DROP TABLE tab;
