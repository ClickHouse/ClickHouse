-- Regression test: make_distributed_plan rejects aggregations it cannot distribute correctly,
-- rather than silently running them single-node.
--   * GROUPING SETS: shuffle scatters by the full key set, so subtotals (over key subsets) would be
--     produced independently in several buckets and duplicated.
--   * A global GROUP BY limit (max_rows_to_group_by) cannot be enforced once split per bucket.

DROP TABLE IF EXISTS t_agg_guard;

CREATE TABLE t_agg_guard (a UInt32, b UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO t_agg_guard SELECT number % 10, number % 7, number FROM numbers(100000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 1000000000, enable_join_runtime_filters = 0;

SELECT '-- GROUPING SETS rejected';
SELECT a, b, sum(v) AS s FROM t_agg_guard GROUP BY GROUPING SETS ((a), (b), ()); -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- max_rows_to_group_by rejected';
SELECT a, sum(v) FROM t_agg_guard GROUP BY a
SETTINGS max_rows_to_group_by = 5; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_agg_guard;
