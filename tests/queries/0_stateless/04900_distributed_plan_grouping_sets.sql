-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `GROUP BY GROUPING SETS` under `make_distributed_plan`. There is no separate plan step: the
-- `Aggregating` step computes every set and tags rows with `__grouping_set`. Only the
-- partial-aggregation strategy is correct for it (a shuffle by the full key set would produce a
-- subtotal group in several buckets), so the plan must show `Aggregating (partial)` plus a merge.

DROP TABLE IF EXISTS t_gs_dist;
CREATE TABLE t_gs_dist (k1 String, k2 UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_gs_dist SELECT 'k' || (number % 3)::String, number % 2, number FROM numbers(1000);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0.
SET max_rows_to_group_by = 0;
-- Pin off: statistics change the estimated group count; grouping sets must not depend on it.
SET use_statistics = 0;
SET explain_query_plan_default = 'legacy';

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SELECT '-- grouping sets, group_by_use_nulls = 0';
SELECT k1, k2, sum(v), count()
FROM t_gs_dist GROUP BY GROUPING SETS ((k1), (k2), ()) ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT '-- grouping sets with grouping(), group_by_use_nulls = 1';
SELECT k1, k2, grouping(k1) + grouping(k2) AS level, sum(v)
FROM t_gs_dist GROUP BY GROUPING SETS ((k1), (k1, k2)) ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '-- distributed plan';
-- Pin off: with memory-efficient merging the plan dump gains a `Mode` line.
EXPLAIN SELECT k1, k2, sum(v) FROM t_gs_dist GROUP BY GROUPING SETS ((k1), (k2)) ORDER BY k1, k2 SETTINGS distributed_aggregation_memory_efficient = 0;

DROP TABLE t_gs_dist;
