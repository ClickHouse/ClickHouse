-- Text index fallback over a sparse column: the bypassed-pattern fallback expression
-- runs over physical columns that may be stored sparse, so its UInt8 result can be
-- ColumnSparse. The direct-read consumer must materialize it before the dense cast.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    k UInt64,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8192, ratio_of_defaults_for_sparse_serialization = 0.5;

-- Mostly-empty text -> 'text' column is serialized sparse.
INSERT INTO tab SELECT number, if(number % 1000 = 0, 'hello world foobar', '') FROM numbers(50000);
OPTIMIZE TABLE tab FINAL;

-- text_index_like_max_postings_to_read = 0 forces the LIKE search to bypass postings and
-- fall back to direct predicate evaluation, exercising fillColumnFallback over the sparse input.
SELECT count() FROM tab WHERE text LIKE '%foobar%'
SETTINGS use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1,
         text_index_like_max_postings_to_read = 0, enable_analyzer = 0, max_rows_to_read = 0;

SELECT count() FROM tab WHERE text LIKE '%foobar%'
SETTINGS use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1,
         text_index_like_max_postings_to_read = 0, enable_analyzer = 1, max_rows_to_read = 0;

-- Pattern that matches nothing: result is an all-default sparse column.
SELECT count() FROM tab WHERE text LIKE '%zzzznomatch%'
SETTINGS use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1,
         text_index_like_max_postings_to_read = 0, enable_analyzer = 0, max_rows_to_read = 0;

DROP TABLE tab;
