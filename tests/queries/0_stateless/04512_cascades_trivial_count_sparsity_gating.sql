-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- The sparsity-filter trivial count produces a `ReadFromPreparedSource` leaf that the
-- Cascades optimizer cannot clone.  Like the plain trivial count, the rewrite must skip
-- itself under `make_distributed_plan` + `enable_cascades_optimizer` so the query counts
-- the rows with a distributed read instead of being rejected.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET optimize_trivial_count_query = 1;
SET optimize_trivial_count_with_sparsity_filter = 1;

DROP TABLE IF EXISTS t_count_sparsity;

CREATE TABLE t_count_sparsity (id UInt64, u UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

INSERT INTO t_count_sparsity SELECT number, if(number % 20 = 0, number, 0) FROM numbers(10000);

SELECT '-- counts match the non-distributed baseline';
SELECT count() FROM t_count_sparsity WHERE u != 0;
SELECT count() FROM t_count_sparsity WHERE u = 0;
SELECT count() FROM t_count_sparsity WHERE u != 0
    SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT '-- the plan is a distributed read, not a prepared single-chunk source';
SET explain_query_plan_default = 'legacy';
EXPLAIN PLAN SELECT count() FROM t_count_sparsity WHERE u != 0;

DROP TABLE t_count_sparsity;
