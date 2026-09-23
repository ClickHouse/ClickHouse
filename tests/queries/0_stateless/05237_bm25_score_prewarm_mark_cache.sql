-- Prewarming the mark cache on insert must not cache an empty marks array for the per-row
-- doc-lengths substream of a BM25 text index: reading `_bm25_score` then threw a logical error.

SET enable_analyzer = 1;
SET allow_experimental_bm25_score_column = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_bm25_prewarm;

CREATE TABLE tab_bm25_prewarm
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_scoring = 1, prewarm_mark_cache = 1, index_granularity = 4;

INSERT INTO tab_bm25_prewarm SELECT number, if(number % 3 = 0, 'raft raft log', 'raft log') FROM numbers(10);

SELECT id, _bm25_score > 0 FROM tab_bm25_prewarm WHERE hasToken(body, 'raft') ORDER BY id;

DROP TABLE tab_bm25_prewarm;
