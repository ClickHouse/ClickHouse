-- Exercises the two paths on which the adaptive aggregator hands the work back to the baseline
-- aggregation. A table that consumes many times the freeze threshold in rows while staying below
-- it in keys gives up on freezing. A frozen table whose staged stream proves to repeat the same
-- keys over and over thaws mid-query: the sampled repeat factor of the staged stream crosses the
-- bound, every thread unfreezes, and the merge combines the local tables with the records staged
-- before the thaw. Both shapes are uniform mid-cardinality streams, where each key repeats far
-- too often for staging to pay. Every cell compares the same query with the feature off and on,
-- so the expected output is a column of 1s.

SET max_threads = 4;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128;
-- The hash-table statistics remember the thaw verdict and later runs of a marked query skip the
-- adaptive engagement, so with them on only the first run of each cell would exercise the thaw.
SET collect_hash_table_stats_during_aggregation = 0;
-- The adaptive gate requires two-level aggregation to be permitted.
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;

SELECT 'Thaw: general aggregate (sum)';
SELECT
    (SELECT sum(s), count() FROM (SELECT toUInt64(number % 20000) AS k, sum(number) AS s FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toUInt64(number % 20000) AS k, sum(number) AS s FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Thaw: pure count (value-staged records)';
SELECT
    (SELECT sum(c), count() FROM (SELECT toUInt64(number % 20000) AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(c), count() FROM (SELECT toUInt64(number % 20000) AS k, count() AS c FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Thaw: multi-argument aggregate (argMin) with compacted staging';
SELECT
    (SELECT sum(s), count() FROM (SELECT toUInt64(number % 20000) AS k, argMin(number % 7, number) AS s FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toUInt64(number % 20000) AS k, argMin(number % 7, number) AS s FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Thaw: String key and String aggregate state';
SELECT
    (SELECT sum(cityHash64(k, s)), count() FROM (SELECT toString(number % 20000) AS k, min(toString(number)) AS s FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(cityHash64(k, s)), count() FROM (SELECT toString(number % 20000) AS k, min(toString(number)) AS s FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Thaw under WITH TOTALS';
SELECT
    (SELECT sum(s), count() FROM (SELECT toUInt64(number % 20000) AS k, sum(number) AS s FROM numbers_mt(2000000) GROUP BY k WITH TOTALS SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toUInt64(number % 20000) AS k, sum(number) AS s FROM numbers_mt(2000000) GROUP BY k WITH TOTALS SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Give-up: repeat-dominated stream below the freeze threshold';
SELECT
    (SELECT sum(u), count() FROM (SELECT toUInt64(number % 50) AS k, uniqExact(number % 100000) AS u FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(u), count() FROM (SELECT toUInt64(number % 50) AS k, uniqExact(number % 100000) AS u FROM numbers_mt(2000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

-- Absolute-value guard: self-comparing cells cannot catch a defect shared by both paths, so pin
-- the adaptive results to analytically-known values over deterministic data.
SELECT 'Analytic guard: thaw shape';
SELECT count(), sum(c), sum(s)
FROM
(
    SELECT toUInt64(number % 20000) AS k, count() AS c, sum(number) AS s
    FROM numbers_mt(2000000)
    GROUP BY k
    SETTINGS enable_adaptive_aggregator = 1
);
SELECT 'Analytic guard: give-up shape';
SELECT count(), sum(c), sum(u)
FROM
(
    SELECT toUInt64(number % 50) AS k, count() AS c, uniqExact(number % 100000) AS u
    FROM numbers_mt(2000000)
    GROUP BY k
    SETTINGS enable_adaptive_aggregator = 1
);
