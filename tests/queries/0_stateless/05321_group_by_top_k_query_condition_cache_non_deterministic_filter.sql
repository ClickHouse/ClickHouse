-- Tags: no-parallel-replicas
-- no-parallel-replicas: the dynamic filter links the aggregation to the local reading step.

-- A filter between the `GROUP BY ... LIMIT n` aggregation and the read that is not deterministic across queries
-- (here `getSetting`) changes which rows reach the top-K heap, and so the boundary on the first key. The granules
-- emptied by the boundary of one query must not be reused from the query condition cache by the next one.

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
SET optimize_move_to_prewhere = 0;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET max_threads = 1;
SET max_block_size = 64;

DROP TABLE IF EXISTS t_group_by_top_k_qcc_non_deterministic;

CREATE TABLE t_group_by_top_k_qcc_non_deterministic (a UInt64, b UInt64) ENGINE = MergeTree
ORDER BY a SETTINGS index_granularity = 64;

INSERT INTO t_group_by_top_k_qcc_non_deterministic SELECT intDiv(number, 64), number FROM numbers(2048);

SET custom_min_b = 1;
SELECT a, count() FROM t_group_by_top_k_qcc_non_deterministic WHERE b >= getSetting('custom_min_b') GROUP BY a ORDER BY a LIMIT 2;

-- The boundary of the query above emptied the granules with `a >= 2`, but these are the ones needed here.
SET custom_min_b = 640;
SELECT a, count() FROM t_group_by_top_k_qcc_non_deterministic WHERE b >= getSetting('custom_min_b') GROUP BY a ORDER BY a LIMIT 2;

DROP TABLE t_group_by_top_k_qcc_non_deterministic;
