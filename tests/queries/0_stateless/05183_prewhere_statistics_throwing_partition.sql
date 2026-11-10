SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET use_statistics = 1, use_statistics_cache = 0, use_statistics_for_part_pruning = 0;
SET materialize_statistics_on_insert = 1, short_circuit_function_evaluation = 'disable';

CREATE TABLE prewhere_statistics_throwing_partition (p Int64, value UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', refresh_statistics_interval = 0;

INSERT INTO prewhere_statistics_throwing_partition VALUES (1, 10), (2, 20), (3, 30);

-- Min/max pruning must reject `p = 1` before the statistics estimator evaluates
-- the partition condition, which would divide by zero for that part.
SELECT sum(value) FROM prewhere_statistics_throwing_partition
WHERE p != 1 AND intDiv(1, p - 1) > 0 AND value % 2 = 0
SETTINGS use_constant_folding_in_index_analysis = 0;

SELECT sum(value) FROM prewhere_statistics_throwing_partition
WHERE p != 1 AND intDiv(1, p - 1) > 0 AND value % 2 = 0
SETTINGS use_constant_folding_in_index_analysis = 1;

DROP TABLE prewhere_statistics_throwing_partition;
