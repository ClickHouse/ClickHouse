-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- One query per check in the fallback decision point (`hasPlanUnsupportedStepForDistributed`):
-- with `distributed_plan_fallback_to_local_execution = 1` (the default) each returns correct
-- results while silently running locally; with the setting disabled the same query throws.
-- The strict-mode setting goes in each query's own SETTINGS clause, never session-wide: a
-- session-wide SET would make the label SELECTs below throw too (a plan with no distributable
-- source legitimately fails the "was distributed" assertion - pinned at the end).
--
-- The stateless-test profile sets `max_rows_to_group_by = 10G` (users.d/limits.yaml), which by
-- itself makes every aggregation fall back; the queries aimed at other checks pin it to 0 so each
-- exercises the path it names. `GLOBAL IN` is deliberately absent: the initiator builds the set
-- and ships its values with the worker tasks, so it distributes and is not a fallback case.

SET enable_analyzer = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_fallback;
CREATE TABLE t_fallback (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_fallback SELECT number % 10, number FROM numbers(1000);

SELECT '-- WITH TOTALS falls back';
SELECT k, sum(v) FROM t_fallback GROUP BY k WITH TOTALS ORDER BY k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    max_rows_to_group_by = 0, log_comment = '05045_fallback_totals';

SELECT '-- max_rows_to_group_by falls back';
SELECT k, sum(v) FROM t_fallback GROUP BY k ORDER BY k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    max_rows_to_group_by = 100;

SELECT '-- _part_index read falls back';
SELECT count(), max(_part_index) >= 0 FROM t_fallback
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    max_rows_to_group_by = 0;

SYSTEM FLUSH LOGS query_log;

-- A fallen-back query must not run through the distributed executor: assert the absence of
-- `DistributedPlanLocalExecution` on the query's own entry.
SELECT '-- fallback ran locally:',
    max(ProfileEvents['DistributedPlanLocalExecution']) = 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = '05045_fallback_totals';

SELECT '-- strict mode throws';
SELECT k, sum(v) FROM t_fallback GROUP BY k WITH TOTALS ORDER BY k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    max_rows_to_group_by = 0,
    distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT k, sum(v) FROM t_fallback GROUP BY k ORDER BY k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    max_rows_to_group_by = 100,
    distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT count(), max(_part_index) >= 0 FROM t_fallback
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    max_rows_to_group_by = 0,
    distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

-- Accepted semantics: strict mode asserts "this query WAS distributed", so a plan with no
-- distributable source at all legitimately fails it too.
SELECT '-- strict mode also refuses a plan with no distributable source';
SELECT 1 SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_fallback;
