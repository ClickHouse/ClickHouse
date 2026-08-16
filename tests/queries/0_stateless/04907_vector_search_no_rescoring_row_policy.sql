-- Tags: no-fasttest, no-ordinary-database

-- A row policy is applied inside `ReadFromMergeTree`, so it is not represented by the explicit
-- filter/PREWHERE node handled by the no-rescoring rewrite. When the policy consumes the vector
-- column, the rewrite must leave the column in the read header for the policy ActionsDAG.

DROP ROW POLICY IF EXISTS 04907_vector_row_policy ON tab_vec_row_policy;
DROP TABLE IF EXISTS tab_vec_row_policy;

CREATE TABLE tab_vec_row_policy
(
    id Int32,
    tenant UInt8,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_vec_row_policy VALUES (0, 1, [1.0, 0.0]), (1, 1, [1.1, 0.0]), (2, 1, [1.2, 0.0]), (3, 1, [1.3, 0.0]), (4, 1, [1.4, 0.0]), (5, 1, [0.0, 2.0]), (6, 1, [0.0, 2.1]), (7, 1, [0.0, 2.2]), (8, 1, [0.0, 2.3]), (9, 1, [0.0, 2.4]);

CREATE ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy
USING length(vec) = 2 AS RESTRICTIVE TO ALL;

SELECT id FROM tab_vec_row_policy
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0;

-- `selectRangesToRead` may defer the row policy for FINAL before the optimizer's second pass.
SELECT id FROM tab_vec_row_policy FINAL
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS
    vector_search_with_rescoring = 0,
    query_plan_optimize_lazy_materialization = 0,
    make_distributed_plan = 1,
    distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0;

-- An explicit PREWHERE is deferred after FINAL before the second optimizer pass.
SELECT id FROM tab_vec_row_policy FINAL
PREWHERE length(vec) = 2
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS
    vector_search_with_rescoring = 0,
    query_plan_optimize_lazy_materialization = 0,
    make_distributed_plan = 1,
    distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0,
    apply_prewhere_after_final = 1;

-- A row-policy DAG also carries all required table columns as passthrough outputs. A policy
-- that does not consume the vector column must not disable the no-rescoring rewrite merely
-- because it carries `vec` through.
DROP ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy;
CREATE ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy
USING tenant = 1 AS RESTRICTIVE TO ALL;

SELECT count() FROM
(
    EXPLAIN SELECT id FROM tab_vec_row_policy
    ORDER BY L2Distance(vec, [0., 2.]) ASC
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0
)
WHERE explain LIKE '%Sort description: sqrt(_distance)%';

-- With FINAL, this non-consuming policy is deferred and its retained `vec` passthrough is
-- replayed by `FilterTransform`. The rewrite must remain valid when that deferred DAG sees the
-- rewritten read header in a local distributed plan.
SELECT count(id) FROM
(
    SELECT id FROM tab_vec_row_policy FINAL
    ORDER BY L2Distance(vec, [0., 2.]) ASC
    LIMIT 3
)
SETTINGS
    vector_search_with_rescoring = 0,
    query_plan_optimize_lazy_materialization = 0,
    make_distributed_plan = 1,
    distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0,
    enable_parallel_replicas = 0,
    max_rows_to_group_by = 0;

DROP ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy;
DROP TABLE tab_vec_row_policy;
