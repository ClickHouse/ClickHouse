-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_bm25_score_column = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_top_k_dynamic_filtering = 1;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE IF EXISTS tab_bm25_topk;

CREATE TABLE tab_bm25_topk
(
    id UInt32,
    body String,
    price UInt32,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_topk VALUES
    (1, 'raft consensus raft log', 5),
    (2, 'consensus protocol basics', 20),
    (3, 'paxos consensus paxos consensus paxos overview', 8),
    (4, 'log replication stream raft', 3),
    (5, 'raft leader election term log', 15),
    (6, 'kv store engine internals', 2),
    (7, 'distributed consensus raft raft raft quorum', 30),
    (8, 'stream processing pipeline notes', 7),
    (9, 'query planner and optimizer', 12),
    (10, 'vector search with quorum reads', 4);

INSERT INTO tab_bm25_topk VALUES
    (11, 'raft consensus deep dive', 9),
    (12, 'unrelated filler document', 1),
    (13, 'consensus with raft in production', 22),
    (14, 'more filler text here', 6),
    (15, 'raft raft raft everywhere', 11);

-- The dynamic top-K prewhere and the direct read from the text index must both apply to the same plan.
SELECT 'top-K dynamic filter applied', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT id, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') ORDER BY _bm25_score DESC, id LIMIT 3
)
WHERE explain LIKE '%__topKFilter(_bm25_score)%';

SELECT 'direct read applied', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT id, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') ORDER BY _bm25_score DESC, id LIMIT 3
)
WHERE explain LIKE '%__text_index_idx_body_hasAnyTokens%';

SELECT 'results desc';
SELECT id, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') ORDER BY _bm25_score DESC, id LIMIT 3;

SELECT 'results desc reference';
SELECT id, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') ORDER BY _bm25_score DESC, id LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

SELECT 'results asc';
SELECT id, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') ORDER BY _bm25_score ASC, id LIMIT 3;

SELECT 'results asc reference';
SELECT id, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') ORDER BY _bm25_score ASC, id LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

SELECT 'results with extra condition';
SELECT id, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') AND id % 2 = 1 ORDER BY _bm25_score DESC, id LIMIT 3;

SELECT 'results with extra condition reference';
SELECT id, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') AND id % 2 = 1 ORDER BY _bm25_score DESC, id LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- Sorting by another column while reading the score: the dynamic filter applies to that column.
SELECT 'top-K on other sort column applied', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT id, price, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') ORDER BY price DESC, id LIMIT 3
)
WHERE explain LIKE '%__topKFilter(price)%';

SELECT 'results other sort column';
SELECT id, price, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') ORDER BY price DESC, id LIMIT 3;

SELECT 'results other sort column reference';
SELECT id, price, round(_bm25_score, 4) FROM tab_bm25_topk WHERE hasAnyTokens(body, 'raft consensus') ORDER BY price DESC, id LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

DROP TABLE tab_bm25_topk;
