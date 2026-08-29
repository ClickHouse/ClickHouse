-- Exercises the adaptive aggregator's admission around pipeline shapes it must stand aside
-- for: results must be identical with the setting on and off whether the feature engages or
-- the admission rejects it. Every cell prints 1.
--
-- Every case below pins `optimize_aggregation_in_order` in its own `SETTINGS` clause, which wins
-- over the randomized client settings. Aggregation in order is the first admission rejection the
-- step checks, so a randomized `optimize_aggregation_in_order = 1` would make every other case
-- reject for that reason instead of the one it means to exercise.

SET max_threads = 4;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
-- The adaptive admission rejects any group-by row limit.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_admission;
CREATE TABLE t_admission (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_admission SELECT number % 10000, number FROM numbers(40000);

SELECT 'Aggregation in order (not admitted)';
SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 1, enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 1, enable_adaptive_aggregator = 1));

-- A limit above the group count keeps the result deterministic while `max_rows_to_group_by`
-- alone already rejects the admission.
SELECT 'Group-by limits (not admitted)';
SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 0, max_rows_to_group_by = 1000000, group_by_overflow_mode = 'throw', enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 0, max_rows_to_group_by = 1000000, group_by_overflow_mode = 'throw', enable_adaptive_aggregator = 1));

SELECT 'Sharded aggregation takes precedence';
SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 0, enable_sharding_aggregator = 1, enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 0, enable_sharding_aggregator = 1, enable_adaptive_aggregator = 1));

SELECT 'Serialized query plan carries the settings';
SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 0, serialize_query_plan = 1, enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 0, serialize_query_plan = 1, enable_adaptive_aggregator = 1));

DROP TABLE t_admission;

-- A query answered from an aggregating projection runs the merge-only step, which the
-- admission rejects.
SELECT 'Projection (merge-only step, not admitted)';
DROP TABLE IF EXISTS t_projection;
CREATE TABLE t_projection
(
    k UInt64,
    v UInt64,
    PROJECTION agg (SELECT k, sum(v) GROUP BY k)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_projection SELECT number % 10000, number FROM numbers(40000);

SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_projection GROUP BY k SETTINGS optimize_aggregation_in_order = 0, optimize_use_projections = 1, enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_projection GROUP BY k SETTINGS optimize_aggregation_in_order = 0, optimize_use_projections = 1, enable_adaptive_aggregator = 1));
DROP TABLE t_projection;

-- The lazy FINAL replacement builds its own aggregation (GROUP BY the sorting key with argMax
-- states) and passes the adaptive settings through; results must match the baseline either way.
-- `enable_analyzer` is set on the outer statement rather than inside the scalar subqueries:
-- `validateAnalyzerSettings` rejects a subquery that changes it away from the context value, which
-- is what the old-analyzer configuration supplies.
SELECT 'Lazy FINAL replacement';
DROP TABLE IF EXISTS t_lazy_final;
CREATE TABLE t_lazy_final (k UInt64, version UInt64, is_deleted UInt8, v UInt64)
ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY k;
INSERT INTO t_lazy_final SELECT number, 1, 0, number FROM numbers(20000);
INSERT INTO t_lazy_final SELECT number, 2, if(number % 10 = 0, 1, 0), number * 2 FROM numbers(10000, 15000);

SELECT
    (SELECT count(), sum(v) FROM t_lazy_final FINAL WHERE k % 7 != 6 SETTINGS optimize_aggregation_in_order = 0, query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0, enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(v) FROM t_lazy_final FINAL WHERE k % 7 != 6 SETTINGS optimize_aggregation_in_order = 0, query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0, enable_adaptive_aggregator = 1)
SETTINGS enable_analyzer = 1;
DROP TABLE t_lazy_final;
