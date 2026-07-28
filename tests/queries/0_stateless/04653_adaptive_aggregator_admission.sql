-- Tags: long

-- Exercises the adaptive aggregator's admission around pipeline shapes it must stand aside
-- for: results must be identical with the setting on and off whether the feature engages or
-- the admission rejects it. Every cell prints 1.

SET max_threads = 4;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;

DROP TABLE IF EXISTS t_admission;
CREATE TABLE t_admission (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_admission SELECT number % 100000, number FROM numbers(400000);

SELECT 'Aggregation in order (not admitted)';
SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 1, enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 1, enable_adaptive_aggregator = 1));

-- A limit above the group count keeps the result deterministic while `max_rows_to_group_by`
-- alone already rejects the admission.
SELECT 'Group-by limits (not admitted)';
SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS max_rows_to_group_by = 1000000, group_by_overflow_mode = 'throw', enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS max_rows_to_group_by = 1000000, group_by_overflow_mode = 'throw', enable_adaptive_aggregator = 1));

SELECT 'Sharded aggregation takes precedence';
SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS enable_sharding_aggregator = 1, enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS enable_sharding_aggregator = 1, enable_adaptive_aggregator = 1));

SELECT 'Serialized query plan carries the settings';
SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS serialize_query_plan = 1, enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS serialize_query_plan = 1, enable_adaptive_aggregator = 1));

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
INSERT INTO t_projection SELECT number % 100000, number FROM numbers(400000);

SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_projection GROUP BY k SETTINGS optimize_use_projections = 1, enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_projection GROUP BY k SETTINGS optimize_use_projections = 1, enable_adaptive_aggregator = 1));
DROP TABLE t_projection;
