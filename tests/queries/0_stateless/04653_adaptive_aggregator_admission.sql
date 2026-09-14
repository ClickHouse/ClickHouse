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
SET log_queries = 1;
SET log_profile_events = 1;

DROP TABLE IF EXISTS t_admission;
CREATE TABLE t_admission (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_admission SELECT number % 10000, number FROM numbers(40000);

SELECT 'Aggregation in order (not admitted)';
SELECT
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 1, enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_admission GROUP BY k SETTINGS optimize_aggregation_in_order = 1, enable_adaptive_aggregator = 1));

-- A group-by limit is admitted in throw mode (05042 covers it), but the dropping modes stay
-- rejected: they leave part of the input unaggregated once a table fills, which the staging
-- has no counterpart for. A limit above the group count keeps the result deterministic,
-- because the break never fires - which also means the exactness pair alone cannot tell a
-- rejection from a wrongly engaged run, so the freeze counter is asserted below on top of it.
-- The arms read `numbers_mt` with a UInt32 key instead of `t_admission`, because the assertion
-- needs a shape the admission would accept if not for the mode: the single-part table reads as
-- one stream, and a `% 10000` key narrows to UInt16, whose fixed hash map has no two-level
-- form - either would get the pair rejected before the mode is even considered.
SELECT 'Group-by limits in a dropping mode (not admitted)';
SELECT
    (SELECT count(), sum(s) FROM (SELECT number % 200000 AS k, sum(number) AS s FROM numbers_mt(400000) GROUP BY k SETTINGS optimize_aggregation_in_order = 0, max_rows_to_group_by = 1000000, group_by_overflow_mode = 'break', enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT number % 200000 AS k, sum(number) AS s FROM numbers_mt(400000) GROUP BY k SETTINGS optimize_aggregation_in_order = 0, max_rows_to_group_by = 1000000, group_by_overflow_mode = 'break', enable_adaptive_aggregator = 1))
SETTINGS log_comment = '04653_break_mode';

-- The group count is far above the freeze threshold, so a wrongly engaged run would have frozen
-- its tables; zero freezes across the whole pair is what proves the break-mode arm was rejected.
-- The same pair with the throw mode does freeze, so the counter is known to see engagement in
-- this exact shape. The assertion can only under-report (with parallel replicas the partial
-- aggregation's events land on the replicas), so it never fails spuriously.
SYSTEM FLUSH LOGS query_log;
SELECT 'break mode stayed on the baseline', coalesce(sum(ProfileEvents['AdaptiveAggregationLocalFreezes']), 0) = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND event_date >= yesterday() AND event_time >= now() - 600
    AND log_comment = '04653_break_mode';

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
