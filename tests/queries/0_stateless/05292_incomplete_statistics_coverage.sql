-- A column statistic must cover every selected part before it drives a relation estimate.
-- Read complete `y` statistics alongside incomplete `v` statistics.

SET allow_statistics = 1;
SET enable_analyzer = 1;
SET mutations_sync = 2, alter_sync = 2;
SET explain_query_plan_default = 'legacy';
SET use_statistics = 1,
    use_statistics_cache = 0,
    use_statistics_for_part_pruning = 0,
    enable_cascades_optimizer = 0,
    enable_parallel_replicas = 0,
    enable_join_runtime_filters = 0,
    query_plan_optimize_join_order_limit = 10,
    query_plan_optimize_join_order_randomize = 0,
    query_plan_optimize_join_order_algorithm = 'greedy',
    query_plan_join_swap_table = 0,
    use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS t_incomplete_statistics;
DROP TABLE IF EXISTS d_incomplete_statistics;

CREATE TABLE t_incomplete_statistics
(
    p UInt8,
    id UInt64,
    v UInt64 STATISTICS(basic),
    y UInt8 STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0;

CREATE TABLE d_incomplete_statistics (id UInt64)
ENGINE = MergeTree
ORDER BY id
SETTINGS auto_statistics_types = '';

-- Start with complete statistics, then clear only v in partition 1. This models
-- a real missing per-column materialization without changing y's coverage.
SET materialize_statistics_on_insert = 1;
INSERT INTO t_incomplete_statistics SELECT 0, number, number, number % 2 FROM numbers(1000);
INSERT INTO t_incomplete_statistics SELECT 1, number + 1000, number + 1000, number % 2 FROM numbers(1000);
INSERT INTO d_incomplete_statistics SELECT number FROM numbers(2000);

ALTER TABLE t_incomplete_statistics CLEAR STATISTICS v IN PARTITION 1;

SELECT 'mixed part-statistics scope is pinned',
    countIf(column = 'v') = 2
        AND countIf(column = 'v' AND length(statistics) > 0) = 1
        AND countIf(column = 'y') = 2
        AND countIf(column = 'y' AND length(statistics) > 0) = 2
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 't_incomplete_statistics'
    AND active;

-- With only part 0's [0,999] v distribution, the old estimator predicts no matches.
-- The complete y statistic selects half the rows; the missing v statistic uses
-- the range fallback (0.33), so the repaired estimate is about 2000 * 0.5 * 0.33.
-- This also fails if y's valid statistic is discarded. The actual result is 250.
SELECT 'partial v falls back while retaining y',
    count() = 1
        AND countIf(toUInt64OrNull(extract(explain, 't_incomplete_statistics\\[[^0-9]*([0-9]+)\\]')) BETWEEN 300 AND 360) = 1
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count()
    FROM t_incomplete_statistics
    INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
    WHERE t_incomplete_statistics.v >= 1500 AND t_incomplete_statistics.y = 0
)
WHERE explain LIKE '%Join:%';

SELECT 'mixed-state SELECT result is 250', count() = 250
FROM t_incomplete_statistics
INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
WHERE t_incomplete_statistics.v >= 1500 AND t_incomplete_statistics.y = 0;

ALTER TABLE t_incomplete_statistics MATERIALIZE STATISTICS v IN PARTITION 1;

SELECT 'complete v statistics cover both parts',
    countIf(column = 'v') = 2
        AND countIf(column = 'v' AND length(statistics) > 0) = 2
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 't_incomplete_statistics'
    AND active;

SELECT 'complete v distribution estimates about 500 rows',
    count() = 1
        AND countIf(toUInt64OrNull(extract(explain, 't_incomplete_statistics\\[[^0-9]*([0-9]+)\\]')) BETWEEN 450 AND 550) = 1
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count()
    FROM t_incomplete_statistics
    INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
    WHERE t_incomplete_statistics.v >= 1500
)
WHERE explain LIKE '%Join:%';

SELECT 'complete-state SELECT result is 500', count() = 500
FROM t_incomplete_statistics
INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
WHERE t_incomplete_statistics.v >= 1500;

DROP TABLE d_incomplete_statistics;
DROP TABLE t_incomplete_statistics;
