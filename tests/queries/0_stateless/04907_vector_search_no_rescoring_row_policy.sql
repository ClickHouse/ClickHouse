-- Tags: no-fasttest, no-ordinary-database

-- A row policy is applied inside `ReadFromMergeTree`, so it is not represented by the explicit
-- filter/PREWHERE node handled by the no-rescoring rewrite. When the policy consumes the vector
-- column, the rewrite must leave the column in the read header for the policy ActionsDAG.

DROP ROW POLICY IF EXISTS 04907_vector_row_policy ON tab_vec_row_policy;
DROP TABLE IF EXISTS tab_vec_row_policy;

CREATE TABLE tab_vec_row_policy
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_vec_row_policy VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

CREATE ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy
USING length(vec) = 2 AS RESTRICTIVE TO ALL;

SELECT id FROM tab_vec_row_policy
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0;

DROP ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy;
DROP TABLE tab_vec_row_policy;
