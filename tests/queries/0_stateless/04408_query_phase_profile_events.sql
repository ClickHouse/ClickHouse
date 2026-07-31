-- Checks the per-phase query pre-execution ProfileEvents:
--   QueryParseMicroseconds, QueryAnalysisMicroseconds,
--   QueryPlanBuildMicroseconds, QueryPipelineBuildMicroseconds.
-- Parsing is measured on every code path; the other three are only emitted on
-- the analyzer path. The test runs under both interpreters (the suite executes
-- it once with the analyzer and once without), so the analyzer-only events are
-- non-zero in 04408_query_phase_profile_events.reference and zero in
-- 04408_query_phase_profile_events.oldanalyzer.reference.

-- A non-trivial query so every phase does measurable work.
SELECT number, number * 2 AS x, toString(number) AS s
FROM numbers(100000)
WHERE number % 3 = 0
GROUP BY number, x, s
ORDER BY number
LIMIT 5
FORMAT Null
SETTINGS log_comment = '04408_query_phase_workload';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['QueryParseMicroseconds'] > 0 AS parse_measured,
    ProfileEvents['QueryAnalysisMicroseconds'] > 0 AS analysis_measured,
    ProfileEvents['QueryPlanBuildMicroseconds'] > 0 AS plan_build_measured,
    ProfileEvents['QueryPipelineBuildMicroseconds'] > 0 AS pipeline_build_measured
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '04408_query_phase_workload'
ORDER BY event_time DESC
LIMIT 1;
