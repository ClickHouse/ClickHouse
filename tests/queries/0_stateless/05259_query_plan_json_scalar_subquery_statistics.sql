-- Tags: no-old-analyzer, no-parallel-replicas

-- The statistics of a scalar subquery are read only once its pipeline has finished.
--
-- `PullingAsyncPipelineExecutor::pull` returning a chunk leaves the pipeline running on a
-- background thread, and the reads that drain it happen further below, after the checks that
-- reject a subquery returning more than one row. Taking the sub-plan as soon as the first chunk
-- arrived read a step clock that had not been stopped yet, so every scalar sub-plan reported an
-- `ExecutionTimeNs` of 0 however much work it had done.

DROP TABLE IF EXISTS t_stats_05259;

CREATE TABLE t_stats_05259 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_stats_05259 SELECT number FROM numbers(1000000);

SET log_query_plans = 1;

SELECT (SELECT sum(k) FROM t_stats_05259) AS s
    SETTINGS log_comment = '05259_scalar' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

WITH JSONExtractRaw(toJSONString(query_plan), 'SubPlans', 1) AS sub_plan
SELECT
    'scalar',
    -- The sub-plan is captured, and its thread count was recorded even before the fix.
    JSONExtractUInt(sub_plan, 'MaxThreads') > 0 AS has_threads,
    -- What the premature read lost: a scan of a million rows takes a measurable time.
    JSONExtractUInt(sub_plan, 'ExecutionTimeNs') > 0 AS has_execution_time
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment = '05259_scalar';

DROP TABLE t_stats_05259;
