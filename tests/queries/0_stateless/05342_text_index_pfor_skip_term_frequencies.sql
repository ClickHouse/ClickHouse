-- Tags: no-parallel-replicas

-- A `pfor` text index with `scoring = 'bm25'` stores a block of term frequencies after every block of row ids.
-- A query without `bm25` reads only the row ids and steps over the term frequency blocks by their headers.

SET enable_analyzer = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET text_index_posting_list_apply_mode = 'materialize';

DROP TABLE IF EXISTS tab_pfor_skip_tf;

CREATE TABLE tab_pfor_skip_tf
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'pfor', scoring = 'bm25')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_scoring = 1;

-- `alpha`: small varied term frequencies with rare large outliers (exception blocks).
-- `beta`: term frequency 1 everywhere (constant blocks).
-- `gamma`: the same large term frequency in every row (constant blocks with a non-zero value).
INSERT INTO tab_pfor_skip_tf
SELECT
    number,
    concat(
        if(number % 3 = 0, repeat('alpha ', if(number % 97 = 0, 300, 1 + number % 5)), ''),
        if(number % 2 = 0, 'beta ', ''),
        if(number % 11 = 0, repeat('gamma ', 40), ''),
        'filler')
FROM numbers(20000);

SELECT 'alpha', count(), sum(id) FROM tab_pfor_skip_tf WHERE hasToken(body, 'alpha');
SELECT 'beta', count(), sum(id) FROM tab_pfor_skip_tf WHERE hasToken(body, 'beta');
SELECT 'gamma', count(), sum(id) FROM tab_pfor_skip_tf WHERE hasToken(body, 'gamma');
SELECT 'alpha and gamma', count(), sum(id) FROM tab_pfor_skip_tf WHERE hasAllTokens(body, ['alpha', 'gamma']);

DROP TABLE tab_pfor_skip_tf;
