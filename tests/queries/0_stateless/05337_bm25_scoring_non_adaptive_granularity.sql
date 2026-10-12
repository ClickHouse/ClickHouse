-- With non-adaptive granularity the writer counts the last granule as full until the part is finalized.
-- Writing the marks of the document-lengths (.dl) stream of a BM25 text index must accept a part
-- whose last granule is incomplete, on insert and on merge.

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_bm25_non_adaptive;

CREATE TABLE tab_bm25_non_adaptive
(
    id UInt64,
    str String,
    INDEX idx_str str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_non_adaptive SELECT number, multiIf(number % 10 = 0, 'error error', number % 10 = 5, 'error noise', 'noise noise') FROM numbers(1);
INSERT INTO tab_bm25_non_adaptive SELECT number + 1, multiIf(number % 10 = 0, 'error error', number % 10 = 5, 'error noise', 'noise noise') FROM numbers(100);

SELECT round(bm25(), 4) AS score, count() FROM tab_bm25_non_adaptive WHERE hasToken(str, 'error') GROUP BY score ORDER BY score;

OPTIMIZE TABLE tab_bm25_non_adaptive FINAL;

SELECT round(bm25(), 4) AS score, count() FROM tab_bm25_non_adaptive WHERE hasToken(str, 'error') GROUP BY score ORDER BY score;

DROP TABLE tab_bm25_non_adaptive;
