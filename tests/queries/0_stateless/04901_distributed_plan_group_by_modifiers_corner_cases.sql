-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Corner cases for GROUP BY modifiers under `make_distributed_plan`: aggregates whose serialized
-- form carries no argument names (with parameters, several arguments, zero arguments), key types
-- that change under `group_by_use_nulls` (`LowCardinality`), empty input, duplicate grouping sets,
-- a multi-step plan with a distributed join below the rollup and a window above it, and the
-- errors for the still unsupported `WITH TOTALS` and extremes.

DROP TABLE IF EXISTS t_corner;
DROP TABLE IF EXISTS t_corner_dim;
CREATE TABLE t_corner (k1 LowCardinality(String), k2 UInt64, k3 UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_corner SELECT 'k' || (number % 3)::String, number % 2, number % 5, number FROM numbers(1000);
CREATE TABLE t_corner_dim (id UInt64, mult UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_corner_dim VALUES (0, 1), (1, 10);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0.
SET max_rows_to_group_by = 0;
-- Pin off: statistics change the estimated group count, flipping the distributed aggregation
-- strategy and thus plan shapes this test relies on.
SET use_statistics = 0;

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SELECT '-- parametrized and multi-argument aggregates over rollup';
SELECT k1, grouping(k1) AS g, quantilesExact(0.5, 0.9)(v) AS q, argMax(k2, v) AS am, count()
FROM t_corner GROUP BY k1 WITH ROLLUP ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '-- LowCardinality key over rollup, group_by_use_nulls = 1';
SELECT k1, toTypeName(k1) AS t, sum(v)
FROM t_corner GROUP BY k1 WITH ROLLUP ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '-- empty input';
SELECT k1, sum(v), count() FROM t_corner WHERE v > 1000000 GROUP BY k1 WITH ROLLUP ORDER BY ALL;
SELECT k1, k2, sum(v) FROM t_corner WHERE v > 1000000 GROUP BY k1, k2 WITH CUBE ORDER BY ALL;

SELECT '-- cube over three keys';
SELECT k1, k2, k3, count() FROM t_corner WHERE v < 20 GROUP BY k1, k2, k3 WITH CUBE ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '-- duplicate grouping sets';
SELECT k1, grouping(k1) AS g, sum(v) FROM t_corner GROUP BY GROUPING SETS ((k1), (k1)) ORDER BY ALL;

SELECT '-- distributed join below rollup, window and limit above it';
SELECT k1, grouping(k1) AS g, sum(v * mult) AS s,
    rank() OVER (PARTITION BY grouping(k1) ORDER BY sum(v * mult) DESC) AS r
FROM t_corner AS a JOIN t_corner_dim AS d ON a.k2 = d.id
GROUP BY k1 WITH ROLLUP
HAVING sum(v * mult) > 0
ORDER BY g, r, k1 LIMIT 20;

SELECT '-- grouping over a wide cube';
SELECT count(), sum(g), min(g), max(g) FROM
(
    SELECT grouping(v % 2) AS g, count()
    FROM t_corner WHERE v < 2
    GROUP BY v % 2, v % 3, v % 4, v % 5, v % 6, v % 7, v % 8, v % 9, v % 10, v % 11, v % 12, v % 13
    WITH CUBE
);

SELECT '-- grouping over a cube with 64 keys is rejected at planning, same as at execution';
EXPLAIN SELECT grouping(v % 2), count() FROM t_corner
GROUP BY
    v % 2, v % 3, v % 4, v % 5, v % 6, v % 7, v % 8, v % 9,
    v % 10, v % 11, v % 12, v % 13, v % 14, v % 15, v % 16, v % 17,
    v % 18, v % 19, v % 20, v % 21, v % 22, v % 23, v % 24, v % 25,
    v % 26, v % 27, v % 28, v % 29, v % 30, v % 31, v % 32, v % 33,
    v % 34, v % 35, v % 36, v % 37, v % 38, v % 39, v % 40, v % 41,
    v % 42, v % 43, v % 44, v % 45, v % 46, v % 47, v % 48, v % 49,
    v % 50, v % 51, v % 52, v % 53, v % 54, v % 55, v % 56, v % 57,
    v % 58, v % 59, v % 60, v % 61, v % 62, v % 63, v % 64, v % 65
WITH CUBE; -- { serverError TOO_MANY_COLUMNS }

SELECT '-- WITH TOTALS and extremes stay fail-closed';
SELECT k1, sum(v) FROM t_corner GROUP BY k1 WITH TOTALS; -- { serverError SUPPORT_IS_DISABLED }
SELECT k1, sum(v) FROM t_corner GROUP BY k1 WITH ROLLUP WITH TOTALS; -- { serverError SUPPORT_IS_DISABLED }
SELECT sum(v) FROM t_corner SETTINGS extremes = 1; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_corner;
DROP TABLE t_corner_dim;
