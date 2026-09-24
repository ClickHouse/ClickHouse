-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: `lowerUTF8` needs ICU.
-- A text index with the `lowerUTF8` preprocessor is not used by `hasTokenPrefix`: context-dependent case mapping
-- (e.g. the Greek final sigma) does not preserve prefixes. The function sees the raw values, with any settings.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

SELECT '-- lowerUTF8 preprocessor: the index is not used';

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lowerUTF8(msg)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, if(number < 8, 'Charged', 'other') FROM numbers(64);

SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'Charg');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'Charg')) WHERE explain LIKE '%Granules:%';
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;

DROP TABLE tab;

SELECT '-- lowerUTF8 preprocessor with the array tokenizer: the index tokenizer is still used';

CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array, preprocessor = lowerUTF8(tag)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, if(number < 8, 'Env:prod-eu', 'env:dev') FROM numbers(64);

SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'Env:prod');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'Env:prod') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'env:prod');

DROP TABLE tab;
