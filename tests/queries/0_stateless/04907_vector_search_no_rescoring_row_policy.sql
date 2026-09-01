-- Tags: no-fasttest, no-ordinary-database, no-old-analyzer, no-parallel-replicas
-- no-old-analyzer: the last query uses a correlated subquery, which only the analyzer supports.
-- The old-analyzer row-policy path is still covered here: `MergeTree` pushes old-analyzer row
-- policies into `ReadFromMergeTree`, so the queries below that pin `enable_analyzer = 0` exercise
-- that carrier explicitly.
-- no-parallel-replicas: the test asserts that the no-rescoring optimization applies, and with
-- parallel replicas the optimization is disabled (vector-search read hints are produced during
-- local index analysis).

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
ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_vec_row_policy VALUES (0, 1, [1.0, 0.0]), (1, 1, [1.1, 0.0]), (2, 1, [1.2, 0.0]), (3, 1, [1.3, 0.0]), (4, 1, [1.4, 0.0]), (5, 1, [0.0, 2.0]), (6, 1, [0.0, 2.1]), (7, 1, [0.0, 2.2]), (8, 1, [0.0, 2.3]), (9, 1, [0.0, 2.4]);

CREATE ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy
USING length(vec) = 2 AS RESTRICTIVE TO ALL;

SELECT id FROM tab_vec_row_policy
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0, make_distributed_plan = 0;

-- The old analyzer builds the row policy in `InterpreterSelectQuery::generateFilterActions` and
-- pushes it into `ReadFromMergeTree` as well, so the consuming policy must keep the vector column
-- there too.
SELECT id FROM tab_vec_row_policy
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0, make_distributed_plan = 0, enable_analyzer = 0;

-- `selectRangesToRead` may defer the row policy for FINAL before the optimizer's second pass.
SELECT id FROM tab_vec_row_policy FINAL
ORDER BY L2Distance(vec, [0., 2.]) ASC
LIMIT 3
SETTINGS
    vector_search_with_rescoring = 0,
    query_plan_optimize_lazy_materialization = 0,
    make_distributed_plan = 1,
    distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0,
    use_skip_indexes_if_final = 1;

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
    apply_prewhere_after_final = 1,
    use_skip_indexes_if_final = 1;

-- A row-policy DAG carries all required table columns as passthrough outputs, including the vector
-- column. Such a passthrough is redundant - the policy does not really consume the vector column -
-- so it is pruned and the no-rescoring rewrite still applies: the plan sorts by `_distance` and the
-- read no longer outputs the vector column.
DROP ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy;
CREATE ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy
USING tenant = 1 AS RESTRICTIVE TO ALL;

SELECT count() FROM
(
    EXPLAIN SELECT id FROM tab_vec_row_policy
    ORDER BY L2Distance(vec, [0., 2.]) ASC
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0, make_distributed_plan = 0
)
WHERE explain LIKE '%Sort description: sqrt(_distance)%'
SETTINGS make_distributed_plan = 0;

-- The same pruning must happen for the storage-pushed old-analyzer row policy.
SELECT count() FROM
(
    EXPLAIN SELECT id FROM tab_vec_row_policy
    ORDER BY L2Distance(vec, [0., 2.]) ASC
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_lazy_materialization = 0, make_distributed_plan = 0, enable_analyzer = 0
)
WHERE explain LIKE '%Sort description: sqrt(_distance)%'
SETTINGS make_distributed_plan = 0, enable_analyzer = 0;

-- With `FINAL`, this non-consuming policy is deferred and replayed by `FilterTransform`. Its
-- redundant vector-column passthrough must be pruned there as well, so the no-rescoring rewrite
-- keeps applying and the plan still sorts by `_distance`.
SELECT count() FROM
(
    EXPLAIN SELECT id FROM tab_vec_row_policy FINAL
    ORDER BY L2Distance(vec, [0., 2.]) ASC
    LIMIT 3
)
WHERE explain LIKE '%Sort description: sqrt(_distance)%'
SETTINGS
    vector_search_with_rescoring = 0,
    query_plan_optimize_lazy_materialization = 0,
    use_skip_indexes_if_final = 1,
    make_distributed_plan = 0;

-- Materializing a decorrelated correlated subquery clones the common subplan. One cloned read
-- applies the no-rescoring rewrite, while its sibling still replays the deferred row policy.
-- The two reads must not share the mutable row-policy DAG.
CREATE TABLE tab_vec_row_policy_keys (id Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_vec_row_policy_keys VALUES (5), (6), (7);

SELECT count() FROM
(
    SELECT id FROM tab_vec_row_policy FINAL
    WHERE EXISTS (SELECT 1 FROM tab_vec_row_policy_keys WHERE tab_vec_row_policy_keys.id = tab_vec_row_policy.id)
    ORDER BY L2Distance(vec, [0., 2.]) ASC
    LIMIT 3
)
SETTINGS
    allow_experimental_correlated_subqueries = 1,
    correlated_subqueries_use_in_memory_buffer = 0,
    vector_search_with_rescoring = 0,
    query_plan_optimize_lazy_materialization = 0,
    use_skip_indexes_if_final = 1,
    make_distributed_plan = 0;

DROP TABLE tab_vec_row_policy_keys;

DROP ROW POLICY 04907_vector_row_policy ON tab_vec_row_policy;
DROP TABLE tab_vec_row_policy;
