-- Tags: no-parallel-replicas
-- no-parallel-replicas: the dynamic filter links the aggregation to the local reading step.

-- `GROUP BY key LIMIT n` drops the rows beyond the top-K boundary with a `__topKFilter` PREWHERE. That shrinks
-- the blocks every step between the read and the aggregation sees, so the filter must not be installed when
-- one of those steps, or the existing PREWHERE, depends on its block (`rowNumberInAllBlocks`, `blockSize`):
-- the aggregates would change, as they do for `ORDER BY key LIMIT n` (see `tryOptimizeTopK`).

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

DROP TABLE IF EXISTS t_gb_dyn_block;

CREATE TABLE t_gb_dyn_block (a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 128;

-- `a` is not the sorting key, so the boundary filters rows inside every granule.
INSERT INTO t_gb_dyn_block SELECT number % 100, number FROM numbers(100000);

SELECT 'control: the filter is installed';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT a, count() FROM t_gb_dyn_block GROUP BY a ORDER BY a LIMIT 3)
WHERE explain LIKE '%\_\_topKFilter(a)%';

SELECT 'stateful expression below the aggregation';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT a, max(rn) FROM (SELECT a, rowNumberInAllBlocks() AS rn FROM t_gb_dyn_block) GROUP BY a ORDER BY a LIMIT 3)
WHERE explain LIKE '%\_\_topKFilter%';
SELECT a, max(rn), count() FROM (SELECT a, rowNumberInAllBlocks() AS rn FROM t_gb_dyn_block) GROUP BY a ORDER BY a LIMIT 3;

SELECT 'block-dependent WHERE';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT a, count() FROM t_gb_dyn_block WHERE rowNumberInAllBlocks() % 2 = 0 GROUP BY a ORDER BY a LIMIT 3)
WHERE explain LIKE '%\_\_topKFilter%';
SELECT a, count() FROM t_gb_dyn_block WHERE rowNumberInAllBlocks() % 2 = 0 GROUP BY a ORDER BY a LIMIT 3;

SELECT 'block-dependent PREWHERE';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT a, count() FROM t_gb_dyn_block PREWHERE blockSize() > 0 GROUP BY a ORDER BY a LIMIT 3)
WHERE explain LIKE '%\_\_topKFilter%';

DROP TABLE t_gb_dyn_block;
