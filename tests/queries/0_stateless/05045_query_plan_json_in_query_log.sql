-- Tags: no-old-analyzer

-- Verifies `system.query_log.query_plan`: which queries get a plan, which do not, and that the
-- stored value is the JSON of the plan that actually ran, with its runtime statistics.
--
-- Rows are matched by joining on query_id against the rows of this test's own database, so the
-- assertions cannot see queries left by an earlier run or by another test running in parallel.
--
-- The column has the JSON type, so it is turned back into text with toJSONString before being
-- inspected: that keeps the assertions independent of how JSON subcolumns are addressed, and the
-- keys inherited from EXPLAIN json=1 (`Node Type`, `Node Id`) contain spaces, which a JSON path
-- would have to quote.
--
-- The plan is stored as a flat array: `{"Root": <id>, "ExecutionTimeNs": <ns>, "MaxThreads": <n>,
-- "Output": [...], "Nodes": [...]}`, with each node naming its children by id. Anything a reader
-- can compute from those is not stored -- see StepStatsJSONPrinter.

SET log_query_plans = 0;
SELECT count() FROM numbers(1000) WHERE number > 900 AND '05045_off' != '' FORMAT Null;

SET log_query_plans = 1;

-- A plain SELECT is captured. A million rows keeps the per-step timings above the resolution of
-- the clock, so the statistics assertions are not racing the noise floor.
SELECT number FROM numbers(1000000) WHERE number % 7 = 0 AND '05045_finish' != '' ORDER BY number DESC LIMIT 3 FORMAT Null;

-- A query failing during execution keeps the plan it was running, but has no statistics: they are
-- collected when the pipeline is finalized, which a failing query never reaches.
SELECT throwIf(number = 5, '05045_throw') FROM numbers(10); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;

-- Nothing is stored while the setting is off, and the column takes its default, an empty object.
SELECT 'off', count(), anyLast(toJSONString(query_plan))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND position(query, '05045_off') > 0;

-- The QueryStart row of a captured query carries no plan: none exists yet at that point.
SELECT 'query_start', count(), anyLast(toJSONString(query_plan))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryStart'
  AND position(query, '05045_finish') > 0;

-- The captured plan is valid JSON, describes the tree that ran, and carries statistics.
SELECT
    'finish',
    count(),
    anyLast(isValidJSON(toJSONString(query_plan))),
    anyLast(JSONExtractUInt(toJSONString(query_plan), 'Version')),
    anyLast(JSONExtractString(toJSONString(query_plan), 'Root')) != '',
    anyLast(JSONHas(toJSONString(query_plan), 'Nodes')),
    anyLast(position(toJSONString(query_plan), '"Statistics"')) > 0,
    anyLast(position(toJSONString(query_plan), 'ReadFromSystemNumbers')) > 0,
    anyLast(position(toJSONString(query_plan), '"WallClockTimeNs"')) > 0,
    anyLast(position(toJSONString(query_plan), 'Filter column: number MOD 7 = 0')) > 0,
    anyLast(position(toJSONString(query_plan), 'Sort description: number DESC')) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND position(query, '05045_finish') > 0;

-- A failed query keeps its plan and reports no statistics at all.
SELECT
    'exception',
    count(),
    anyLast(isValidJSON(toJSONString(query_plan))),
    anyLast(JSONExtractString(toJSONString(query_plan), 'Root')) != '',
    anyLast(position(toJSONString(query_plan), '"Statistics"')) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
  AND position(query, '05045_throw') > 0;
