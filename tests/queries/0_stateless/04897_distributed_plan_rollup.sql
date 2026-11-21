-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `GROUP BY ... WITH ROLLUP` under `make_distributed_plan`: the aggregation runs distributed and
-- the `Rollup` step computes the subtotals over the merged result. The `Rollup` step can land in a
-- worker stage (for example below a distributed `ORDER BY`), so its serialization is exercised here.
-- `grouping` is not used: its internal specializations cannot be shipped in a serialized plan yet.

DROP TABLE IF EXISTS t_rollup_dist;
CREATE TABLE t_rollup_dist (k1 String, k2 UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rollup_dist SELECT 'k' || (number % 3)::String, number % 2, number FROM numbers(1000);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0.
SET max_rows_to_group_by = 0;
-- Pin off: statistics change the estimated group count, flipping the distributed aggregation
-- strategy (Shuffle vs partial+merge) and thus the asserted plan.
SET use_statistics = 0;
SET explain_query_plan_default = 'legacy';

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SELECT '-- rollup, group_by_use_nulls = 0';
SELECT k1, k2, sum(v), count()
FROM t_rollup_dist GROUP BY k1, k2 WITH ROLLUP ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT '-- rollup, group_by_use_nulls = 1';
SELECT k1, k2, sum(v), count()
FROM t_rollup_dist GROUP BY k1, k2 WITH ROLLUP ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '-- distributed plan';
-- Pin off: with memory-efficient merging the plan dump gains a `Mode` line.
EXPLAIN SELECT k1, k2, sum(v) FROM t_rollup_dist GROUP BY k1, k2 WITH ROLLUP ORDER BY k1, k2 SETTINGS distributed_aggregation_memory_efficient = 0;

DROP TABLE t_rollup_dist;
