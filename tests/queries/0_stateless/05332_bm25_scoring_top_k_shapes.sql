-- Tags: no-parallel-replicas

-- `ORDER BY bm25() LIMIT n`: the dynamic top-k filter is built by the direct read from the text index as a
-- PREWHERE computing the score. Check the query shapes around it: the filter applies with masked conjunctions,
-- aliases and parameters, and is skipped (with correct results) when the query has a PREWHERE of its own.

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_top_k_dynamic_filtering = 1;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE IF EXISTS tab_bm25_shapes;

CREATE TABLE tab_bm25_shapes
(
    id UInt32,
    body String,
    price UInt32,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_shapes VALUES
    (1, 'raft consensus raft log', 5),
    (2, 'consensus protocol basics', 20),
    (3, 'paxos consensus paxos consensus paxos overview', 8),
    (4, 'log replication stream raft', 3),
    (5, 'raft leader election term log', 15),
    (6, 'kv store engine internals', 2),
    (7, 'distributed consensus raft raft raft quorum', 30),
    (8, 'stream processing pipeline notes', 7),
    (9, 'query planner and optimizer', 12),
    (10, 'vector search with quorum reads', 4),
    (11, 'raft consensus deep dive', 9),
    (12, 'consensus with raft in production', 22),
    (13, 'raft raft raft everywhere', 11);

SELECT '-- masked conjunction over a regular column';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM tab_bm25_shapes WHERE (hasToken(body, 'raft') AND price > 5) OR hasToken(body, 'consensus') ORDER BY bm25() DESC, id LIMIT 3
)
WHERE explain LIKE '%__topKFilter(%';

SELECT id, price, round(bm25(), 4) FROM tab_bm25_shapes WHERE (hasToken(body, 'raft') AND price > 5) OR hasToken(body, 'consensus') ORDER BY bm25() DESC, id LIMIT 3;
SELECT id, price, round(bm25(), 4) FROM tab_bm25_shapes WHERE (hasToken(body, 'raft') AND price > 5) OR hasToken(body, 'consensus') ORDER BY bm25() DESC, id LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0;

SELECT '-- sort by an alias of the score';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id, bm25() AS score FROM tab_bm25_shapes WHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY score DESC, id LIMIT 3
)
WHERE explain LIKE '%__topKFilter(%';

SELECT id, round(bm25(), 4) AS score FROM tab_bm25_shapes WHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY score DESC, id LIMIT 3;
SELECT id, round(bm25(), 4) AS score FROM tab_bm25_shapes WHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY score DESC, id LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0;

SELECT '-- explicit parameters';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM tab_bm25_shapes WHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY bm25(1.5, 0.5) DESC, id LIMIT 3
)
WHERE explain LIKE '%__topKFilter(%';

SELECT id, round(bm25(1.5, 0.5), 4) FROM tab_bm25_shapes WHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY bm25(1.5, 0.5) ASC, id LIMIT 3;
SELECT id, round(bm25(1.5, 0.5), 4) FROM tab_bm25_shapes WHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY bm25(1.5, 0.5) ASC, id LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0;

SELECT '-- PREWHERE on a regular column: no dynamic filter on the score';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM tab_bm25_shapes PREWHERE price >= 5 WHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY bm25() DESC, id LIMIT 3
)
WHERE explain LIKE '%__topKFilter(%';

SELECT id, round(bm25(), 4) FROM tab_bm25_shapes PREWHERE price >= 5 WHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY bm25() DESC, id LIMIT 3;
SELECT id, round(bm25(), 4) FROM tab_bm25_shapes PREWHERE price >= 5 WHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY bm25() DESC, id LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0;

SELECT '-- scoring predicate in PREWHERE: no dynamic filter on the score';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM tab_bm25_shapes PREWHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY bm25() DESC, id LIMIT 3
)
WHERE explain LIKE '%__topKFilter(%';

SELECT id, round(bm25(), 4) FROM tab_bm25_shapes PREWHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY bm25() DESC, id LIMIT 3;
SELECT id, round(bm25(), 4) FROM tab_bm25_shapes PREWHERE hasAnyTokens(body, ['consensus', 'raft']) ORDER BY bm25() DESC, id LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0;

DROP TABLE tab_bm25_shapes;
