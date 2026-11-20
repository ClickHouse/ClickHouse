-- Tags: no-parallel-replicas
-- no-parallel-replicas: the dynamic filter links the aggregation to the local reading step.

-- The `__topKFilter` PREWHERE of `GROUP BY key LIMIT n` shrinks the blocks every step between the read and the
-- aggregation sees, so it must not be installed when one of those steps depends on its block, also when the
-- block-dependent function is inside the body of a lambda. The boundary on the first key depends on all grouping
-- keys, so a block-dependent non-first key is enough to change the result.

SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering = 1;
SET optimize_aggregation_in_order = 0;
SET optimize_trivial_group_by_limit_query = 0;
SET use_query_condition_cache = 0;
SET max_threads = 1;
SET max_block_size = 1024;

DROP TABLE IF EXISTS t_gb_dyn_lambda;

CREATE TABLE t_gb_dyn_lambda (a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 128;

-- `a` is not the sorting key, so the boundary filters rows inside every granule.
INSERT INTO t_gb_dyn_lambda SELECT number % 100, number FROM numbers(100000);

SELECT 'control: the filter is installed';
SELECT count() > 0 FROM (EXPLAIN actions = 1
    SELECT a, g, count() FROM (SELECT a, arrayMap(x -> x + 1, [b]) AS g FROM t_gb_dyn_lambda)
    GROUP BY a, g ORDER BY a, g LIMIT 3)
WHERE explain LIKE '%\_\_topKFilter(a)%';

SELECT 'block-dependent lambda in a non-first key';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT a, g, count() FROM (SELECT a, arrayMap(x -> rowNumberInAllBlocks(), [b]) AS g FROM t_gb_dyn_lambda)
    GROUP BY a, g ORDER BY a, g LIMIT 3)
WHERE explain LIKE '%\_\_topKFilter%';
SELECT a, g, count() FROM (SELECT a, arrayMap(x -> rowNumberInAllBlocks(), [b]) AS g FROM t_gb_dyn_lambda)
GROUP BY a, g ORDER BY a, g LIMIT 3;

DROP TABLE t_gb_dyn_lambda;
