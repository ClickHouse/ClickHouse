-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `grouping` under `make_distributed_plan`. The analyzer resolves it into a specialization
-- (`__groupingForRollup` etc.) whose parameters travel as trailing constant arguments, so a
-- serialized plan can rebuild the function from its name and arguments alone. One of the
-- constants bakes in `force_grouping_standard_compatibility`.

DROP TABLE IF EXISTS t_grouping_dist;
-- Pin the granularity: the EXPLAIN below prints the granule count of the read.
CREATE TABLE t_grouping_dist (k1 String, k2 UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192;
INSERT INTO t_grouping_dist SELECT 'k' || (number % 3)::String, number % 2, number FROM numbers(1000);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0.
SET max_rows_to_group_by = 0;
-- Pin off: statistics change the estimated group count, flipping the distributed aggregation
-- strategy (Shuffle vs partial+merge) and thus the asserted plan.
SET use_statistics = 0;
SET explain_query_plan_default = 'legacy';

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SELECT '-- grouping over rollup';
SELECT k1, k2, grouping(k1) + grouping(k2) AS level, sum(v)
FROM t_grouping_dist GROUP BY k1, k2 WITH ROLLUP ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT '-- grouping over rollup, group_by_use_nulls = 1';
SELECT k1, k2, grouping(k1) + grouping(k2) AS level, sum(v)
FROM t_grouping_dist GROUP BY k1, k2 WITH ROLLUP ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '-- grouping over rollup, force_grouping_standard_compatibility = 0';
SELECT k1, grouping(k1) AS g, sum(v)
FROM t_grouping_dist GROUP BY k1 WITH ROLLUP ORDER BY ALL
SETTINGS force_grouping_standard_compatibility = 0;

SELECT '-- grouping in HAVING';
SELECT k1, sum(v)
FROM t_grouping_dist GROUP BY k1 WITH ROLLUP HAVING grouping(k1) = 1 ORDER BY ALL;

SELECT '-- grouping over plain GROUP BY';
SELECT k1, grouping(k1) AS g, sum(v)
FROM t_grouping_dist GROUP BY k1 ORDER BY ALL;

SELECT '-- union of two rollups with different key counts';
SELECT * FROM (
    SELECT 'a' AS src, k2, toUInt64(0) AS kk, grouping(k2) AS g, sum(v) AS s
    FROM t_grouping_dist GROUP BY k2 WITH ROLLUP
    UNION ALL
    SELECT 'b' AS src, k2, cityHash64(k1) % 2 AS kk, grouping(k2) + grouping(kk) AS g, sum(v) AS s
    FROM t_grouping_dist GROUP BY k2, kk WITH ROLLUP
) ORDER BY ALL;

SELECT '-- rollup over a rollup subquery; the outer grouping has another key count';
-- The inner grouping call keeps its unused argument columns alive as DAG inputs; the rewrite must
-- not drop them, or the serialized step's input header no longer matches the stream.
SELECT k2, g_inner, sum(s) AS total, grouping(k2) AS go
FROM (
    SELECT k1, k2, grouping(k1) + grouping(k2) AS g_inner, sum(v) AS s
    FROM t_grouping_dist GROUP BY k1, k2 WITH ROLLUP
)
GROUP BY k2, g_inner WITH ROLLUP
ORDER BY ALL;

SELECT '-- distributed plan shows the specialization with its constant arguments';
-- Pin off: with memory-efficient merging the plan dump gains a `Mode` line.
EXPLAIN actions = 1 SELECT k1, grouping(k1) AS g, sum(v) FROM t_grouping_dist GROUP BY k1 WITH ROLLUP ORDER BY k1 SETTINGS distributed_aggregation_memory_efficient = 0;

DROP TABLE t_grouping_dist;
