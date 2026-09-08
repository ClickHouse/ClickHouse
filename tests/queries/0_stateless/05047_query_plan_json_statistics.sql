-- Tags: no-old-analyzer

-- Verifies that the statistics stored in `system.query_log.query_plan` are the numbers the query
-- actually produced, not merely that the keys are present. `05045` checks the shape; this checks
-- the values.
--
-- The query is deliberately the shallowest plan that still reads rows -- one Expression over one
-- source -- so the source step is at a fixed path and the row count is exact. `log_comment`
-- identifies the row, leaving the query itself free of markers that would add steps to its plan.

SET log_query_plans = 1;
SELECT number FROM numbers(12345) SETTINGS log_comment = '05047_stats' FORMAT Null;
SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    'io',
    count(),
    -- The source produced exactly the rows asked of it, and the root passed them through.
    anyLast(JSONExtractUInt(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'IO', 'OutputRows')),
    anyLast(JSONExtractUInt(toJSONString(query_plan), 'Statistics', 'IO', 'InputRows')),
    anyLast(JSONExtractUInt(toJSONString(query_plan), 'Statistics', 'IO', 'OutputRows')),
    -- 12345 UInt64 values.
    anyLast(JSONExtractUInt(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'IO', 'OutputBytes'))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05047_stats';

-- The timings are not deterministic, so assert the invariants that must hold of any measurement
-- rather than the values: a step that ran took a positive amount of wall clock, it cannot have
-- taken more than the query did, and the per-processor distribution has to be ordered.
SELECT
    'timings',
    anyLast(JSONExtractUInt(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'Stages', 1, 'WallClockTimeNs')) > 0,
    anyLast(JSONExtractFloat(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'Stages', 1, 'ShareOfQueryTime')) > 0,
    anyLast(JSONExtractFloat(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'Stages', 1, 'ShareOfQueryTime')) <= 100,
    anyLast(JSONExtractFloat(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'Stages', 1, 'Parallelism')) > 0,
    anyLast(JSONExtractUInt(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'Stages', 1, 'Processors')) > 0,
    anyLast(JSONExtractUInt(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'Stages', 1, 'ProcessorTimeNs', 'Min'))
        <= anyLast(JSONExtractUInt(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'Stages', 1, 'ProcessorTimeNs', 'Max')),
    anyLast(JSONExtractUInt(toJSONString(query_plan), 'Plans', 1, 'Statistics', 'Stages', 1, 'ProcessorTimeNs', 'Sum')) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05047_stats';
