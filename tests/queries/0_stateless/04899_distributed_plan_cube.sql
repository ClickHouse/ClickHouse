-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `GROUP BY ... WITH CUBE` under `make_distributed_plan`: the aggregation runs distributed and
-- the `Cube` step computes all subtotal combinations over the merged result. The `Cube` step can
-- land in a worker stage (below a distributed `ORDER BY`), so its serialization is exercised here.

DROP TABLE IF EXISTS t_cube_dist;
CREATE TABLE t_cube_dist (k1 String, k2 UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cube_dist SELECT 'k' || (number % 3)::String, number % 2, number FROM numbers(1000);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0.
SET max_rows_to_group_by = 0;
-- Pin off: statistics change the estimated group count, flipping the distributed aggregation
-- strategy (Shuffle vs partial+merge) and thus the asserted plan.
SET use_statistics = 0;
SET explain_query_plan_default = 'legacy';

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SELECT '-- cube, group_by_use_nulls = 0';
SELECT k1, k2, sum(v), count()
FROM t_cube_dist GROUP BY k1, k2 WITH CUBE ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT '-- cube with grouping(), group_by_use_nulls = 1';
SELECT k1, k2, grouping(k1) + grouping(k2) AS level, sum(v)
FROM t_cube_dist GROUP BY k1, k2 WITH CUBE ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '-- multi-argument grouping in reverse key order';
SELECT k1, k2, grouping(k2, k1) AS g, sum(v)
FROM t_cube_dist GROUP BY k1, k2 WITH CUBE ORDER BY ALL;

SELECT '-- distributed plan';
-- Pin off: with memory-efficient merging the plan dump gains a `Mode` line.
EXPLAIN SELECT k1, k2, sum(v) FROM t_cube_dist GROUP BY k1, k2 WITH CUBE ORDER BY k1, k2 SETTINGS distributed_aggregation_memory_efficient = 0;

DROP TABLE t_cube_dist;
