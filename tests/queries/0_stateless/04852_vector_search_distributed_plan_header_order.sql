-- Tags: no-fasttest, no-ordinary-database, no-old-analyzer
-- The no-rescoring vector search rewrite used to move the distance expression to the end of the
-- `ExpressionStep` output header. Steps created above the `Sorting` before that rewrite runs (the
-- local top-N `Limit` and the exchanges of a distributed plan) kept the original column order, and
-- `makeDistributedPlan` failed to rebuild the plan fragments with a logical error
-- ("Cannot add step Limit to QueryPlan because it has incompatible header with root step Sorting").

DROP TABLE IF EXISTS tab_dist_header;

CREATE TABLE tab_dist_header
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_dist_header VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

-- Only the row count is asserted: the query used to fail at plan build time, before returning
-- anything. The distributed plan currently returns an incorrect top-N for vector search queries
-- regardless of the rescoring mode (a separate pre-existing bug), so the exact rows are not pinned.
-- `count(id)` rather than `count()` so that `id` is not pruned from the inner fragment: the bug needs
-- at least two columns in the `Sorting` output header for the rewrite to change their order.
SELECT count(id) FROM
(
    WITH [0., 2.] AS reference_vec
    SELECT id FROM tab_dist_header
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
)
SETTINGS vector_search_with_rescoring = 0,
    make_distributed_plan = 1,
    distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0,
    enable_parallel_replicas = 0;

DROP TABLE tab_dist_header;
