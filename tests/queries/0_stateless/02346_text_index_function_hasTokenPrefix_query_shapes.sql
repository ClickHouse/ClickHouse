-- Tags: no-parallel-replicas
-- Without the tokenizer argument, `hasTokenPrefix` uses the index tokenizer only where the plan evaluates it over the table,
-- and `splitByNonAlpha` after GROUP BY and in mutations, as `hasAnyTokens` does. It never applies the index preprocessor.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;
SET mutations_sync = 2;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

-- 'prod' has a token starting with 'prod' for both tokenizers, 'env:prod-eu' only for `splitByNonAlpha`.
INSERT INTO tab SELECT number, multiIf(number < 8, 'env:prod-eu', number < 16, 'prod', 'env:dev') FROM numbers(64);

SELECT '-- filter and SELECT list: the index tokenizer';
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod');
SELECT count() FROM tab WHERE hasTokenPrefix(tag, 'prod') SETTINGS use_skip_indexes = 0;
SELECT countIf(hasTokenPrefix(tag, 'prod')) FROM tab;

SELECT '-- after GROUP BY: splitByNonAlpha, unless the tokenizer is explicit';
SELECT tag, hasTokenPrefix(tag, 'prod') FROM tab GROUP BY tag ORDER BY tag;
SELECT tag, hasTokenPrefix(tag, 'prod', 'array') FROM tab GROUP BY tag ORDER BY tag;

SELECT '-- HAVING on the grouping key: the index tokenizer only if the condition is pushed down to the table';
SELECT count() FROM (SELECT tag FROM tab GROUP BY tag HAVING hasTokenPrefix(tag, 'prod')) SETTINGS query_plan_filter_push_down = 1;
SELECT count() FROM (SELECT tag FROM tab GROUP BY tag HAVING hasTokenPrefix(tag, 'prod')) SETTINGS query_plan_filter_push_down = 0;
SELECT count() FROM (SELECT tag FROM tab GROUP BY tag HAVING hasTokenPrefix(tag, 'prod', 'array')) SETTINGS query_plan_filter_push_down = 0;

SELECT '-- ALTER TABLE DELETE: splitByNonAlpha, unless the tokenizer is explicit';
ALTER TABLE tab DELETE WHERE hasTokenPrefix(tag, 'prod');
SELECT count() FROM tab;

TRUNCATE TABLE tab;
INSERT INTO tab SELECT number, multiIf(number < 8, 'env:prod-eu', number < 16, 'prod', 'env:dev') FROM numbers(64);
ALTER TABLE tab DELETE WHERE hasTokenPrefix(tag, 'prod', 'array');
SELECT count() FROM tab;

DROP TABLE tab;

SELECT '-- lower preprocessor: the raw values in every shape';

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

SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'charg');
SELECT count() FROM tab WHERE hasTokenPrefix(msg, 'Charg');
SELECT msg, hasTokenPrefix(msg, 'charg'), hasTokenPrefix(msg, 'Charg') FROM tab GROUP BY msg ORDER BY msg;
ALTER TABLE tab DELETE WHERE hasTokenPrefix(msg, 'charg');
SELECT count() FROM tab;
ALTER TABLE tab DELETE WHERE hasTokenPrefix(msg, 'Charg');
SELECT count() FROM tab;

DROP TABLE tab;
