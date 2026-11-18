-- Tags: no-parallel-replicas
-- no-parallel-replicas: the dynamic filter links the aggregation to the local reading step.

-- The boundary of the `GROUP BY ... LIMIT n` top-K heap on the first key column depends on every grouping key:
-- `GROUP BY a, b` and `GROUP BY a, c` over the same rows reach different boundaries on `a`. The granules emptied
-- by the boundary of one query must not be reused from the query condition cache by the other one.

SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering = 1;
SET optimize_aggregation_in_order = 0;
SET optimize_trivial_group_by_limit_query = 0;
SET optimize_read_in_order = 0;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET max_threads = 1;
SET max_block_size = 64;

DROP TABLE IF EXISTS t_group_by_top_k_qcc_keys;

-- For `a = 1`, `b` has many distinct values and `c` has one; for `a = 2`, both have several.
CREATE TABLE t_group_by_top_k_qcc_keys (a UInt64, b UInt64, c UInt64, s String) ENGINE = MergeTree
ORDER BY a SETTINGS index_granularity = 64;

INSERT INTO t_group_by_top_k_qcc_keys SELECT 1 + intDiv(number, 1024), number, if(number < 1024, 0, number % 3), toString(number) FROM numbers(2048);

-- The boundary is `a = 1`: the granules with `a = 2` are emptied.
SELECT a, b FROM t_group_by_top_k_qcc_keys WHERE s != 'x' GROUP BY a, b ORDER BY a, b LIMIT 2;
-- `a = 1` holds a single group, so the boundary is `a = 2`, and the granules with `a = 2` are needed.
SELECT a, c FROM t_group_by_top_k_qcc_keys WHERE s != 'x' GROUP BY a, c ORDER BY a, c LIMIT 2;

DROP TABLE t_group_by_top_k_qcc_keys;
