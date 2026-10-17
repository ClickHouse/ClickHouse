-- With non-adaptive granularity the writer counts the last granule as full until the part is finalized.
-- Writing the marks of the document-lengths (.dl) stream of a text index with `scoring = 'bm25'` must accept a part
-- whose last granule is incomplete, on insert and on merge.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab_scoring_non_adaptive;

CREATE TABLE tab_scoring_non_adaptive
(
    id UInt64,
    str String,
    INDEX idx_str str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_scoring_non_adaptive SELECT number, multiIf(number % 10 = 0, 'error error', number % 10 = 5, 'error noise', 'noise noise') FROM numbers(1);
INSERT INTO tab_scoring_non_adaptive SELECT number + 1, multiIf(number % 10 = 0, 'error error', number % 10 = 5, 'error noise', 'noise noise') FROM numbers(100);

SELECT weight, count() FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_non_adaptive, idx_str) WHERE token = 'error' GROUP BY weight ORDER BY weight;

OPTIMIZE TABLE tab_scoring_non_adaptive FINAL;

SELECT weight, count() FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_non_adaptive, idx_str) WHERE token = 'error' GROUP BY weight ORDER BY weight;

CHECK TABLE tab_scoring_non_adaptive SETTINGS check_query_single_value_result = 1;

DROP TABLE tab_scoring_non_adaptive;
