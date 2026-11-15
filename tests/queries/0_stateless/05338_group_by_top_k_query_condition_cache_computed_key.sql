-- Tags: no-parallel-replicas
-- no-parallel-replicas: the dynamic filter links the aggregation to the local reading step.

-- Only the first key of `GROUP BY ... LIMIT n` has to reach the aggregation unchanged; the other keys may be
-- computed, and the boundary on the first key depends on them. Two queries that differ only in the expression
-- of a computed key must not share query condition cache entries, even when the key has the same name in both
-- (a column of a subquery).

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

DROP TABLE IF EXISTS t_group_by_top_k_qcc_computed_key;

CREATE TABLE t_group_by_top_k_qcc_computed_key (a UInt64, b UInt64, s String) ENGINE = MergeTree
ORDER BY a SETTINGS index_granularity = 64;

INSERT INTO t_group_by_top_k_qcc_computed_key SELECT 1 + intDiv(number, 1024), number, toString(number) FROM numbers(2048);

-- With `m = 100`, `a = 1` holds many groups, so the boundary is `a = 1`, and the granules with `a = 2` are emptied.
SET param_m = 100;
SELECT a, modulo(b, {m:UInt64}) AS g, count() FROM t_group_by_top_k_qcc_computed_key WHERE s != 'x' GROUP BY a, g ORDER BY a, g LIMIT 2;
-- With `m = 1`, `a = 1` holds a single group, so the boundary is `a = 2`, and the granules with `a = 2` are needed.
SET param_m = 1;
SELECT a, modulo(b, {m:UInt64}) AS g, count() FROM t_group_by_top_k_qcc_computed_key WHERE s != 'x' GROUP BY a, g ORDER BY a, g LIMIT 2;

-- The same with literals under one alias.
SELECT a, modulo(b, 50) AS h, count() FROM t_group_by_top_k_qcc_computed_key WHERE s != 'y' GROUP BY a, h ORDER BY a, h LIMIT 2;
SELECT a, modulo(b, 1) AS h, count() FROM t_group_by_top_k_qcc_computed_key WHERE s != 'y' GROUP BY a, h ORDER BY a, h LIMIT 2;

-- The same through a subquery, where the key is named after the column of the subquery, not after its expression.
SELECT a, g, count() FROM (SELECT a, modulo(b, 100) AS g FROM t_group_by_top_k_qcc_computed_key WHERE s != 'z') GROUP BY a, g ORDER BY a, g LIMIT 2;
SELECT a, g, count() FROM (SELECT a, modulo(b, 1) AS g FROM t_group_by_top_k_qcc_computed_key WHERE s != 'z') GROUP BY a, g ORDER BY a, g LIMIT 2;

DROP TABLE t_group_by_top_k_qcc_computed_key;
