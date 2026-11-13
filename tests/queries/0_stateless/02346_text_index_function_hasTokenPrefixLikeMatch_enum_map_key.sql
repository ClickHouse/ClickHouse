-- Tags: no-parallel-replicas
-- `optimize_functions_to_subcolumns` reads `m[1]` of a `Map(Enum8, String)` as the subcolumn `m.key_prod`.
-- The functions must still find the index on `m[1]` and use its tokenizer, so the result does not change.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    m Map(Enum8('prod' = 1, 'dev' = 2), String),
    INDEX idx(m[1]) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', text_index_posting_list_block_size = 1048576;

INSERT INTO tab SELECT number, map(CAST(1, 'Enum8(\'prod\' = 1, \'dev\' = 2)'), 'env:prod-eu') FROM numbers(64);

SELECT count() FROM tab WHERE hasTokenPrefix(m[1], 'env:prod');
SELECT count() FROM tab WHERE hasTokenPrefix(m[1], 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(m[1], 'env:prod') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab WHERE hasTokenPrefix(m[1], 'env:prod', 'array');
SELECT count() FROM tab WHERE hasTokenLike(m[1], 'env:prod-%');
SELECT count() FROM tab WHERE hasTokenLike(m[1], 'env:prod-%') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenLike(m[1], 'env:prod-%') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab WHERE hasTokenLike(m[1], 'env:prod-%') SETTINGS text_index_like_max_matched_tokens = 1;
SELECT count() FROM tab WHERE hasTokenMatch(m[1], '^env:prod-(eu|us)$');
SELECT count() FROM tab WHERE hasTokenMatch(m[1], '^env:prod-(eu|us)$') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasTokenMatch(m[1], '^env:prod-(eu|us)$') SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE tab;
