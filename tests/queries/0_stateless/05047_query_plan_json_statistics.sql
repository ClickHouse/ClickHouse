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
    -- The source produced exactly the rows asked of it, and the root passed them through. The
    -- nodes are addressed by type rather than by position: a flat array has no guaranteed order,
    -- and that is also the shape the column exists for -- every step reachable without knowing
    -- how deep it sits.
    anyLast(JSONExtractUInt(source, 'Statistics', 'IO', 'OutputRows')),
    anyLast(JSONExtractUInt(root, 'Statistics', 'IO', 'InputRows')),
    anyLast(JSONExtractUInt(root, 'Statistics', 'IO', 'OutputRows')),
    -- 12345 UInt64 values.
    anyLast(JSONExtractUInt(source, 'Statistics', 'IO', 'OutputBytes')),
    -- Two steps, and the root names the one below it.
    anyLast(length(nodes)),
    anyLast(JSONExtractString(arrayElement(JSONExtractArrayRaw(root, 'Children'), 1))) = anyLast(JSONExtractString(source, 'Node Id'))
FROM
(
    SELECT
        JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes') AS nodes,
        arrayFilter(n -> JSONExtractString(n, 'Node Type') = 'ReadFromSystemNumbers', nodes)[1] AS source,
        arrayFilter(n -> JSONExtractString(n, 'Node Id') = JSONExtractString(toJSONString(query_plan), 'Root'), nodes)[1] AS root
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05047_stats'
);

-- The timings are not deterministic, so assert the invariants that must hold of any measurement
-- rather than the values: a step that ran took a positive amount of wall clock, it cannot have
-- taken more than the query did, and the per-processor distribution has to be ordered.
--
-- Nothing derivable is stored, so nothing derivable is asserted here. What used to be checked as
-- `ShareOfQueryTime` and `Parallelism` is checked as the quantities they are computed from --
-- the query-level totals at the root and the per-stage primaries -- which is all a reader has.
SELECT
    'timings',
    anyLast(JSONExtractUInt(stage, 'WallClockTimeNs')) > 0,
    anyLast(JSONExtractUInt(stage, 'Processors')) > 0,
    anyLast(JSONExtractUInt(stage, 'ProcessorTimeNs', 'Min')) <= anyLast(JSONExtractUInt(stage, 'ProcessorTimeNs', 'Max')),
    anyLast(JSONExtractUInt(stage, 'ProcessorTimeNs', 'Sum')) > 0,
    -- The query-level figures every stage is read against. A step cannot have been busy for
    -- longer than the query executed, which is what keeps a derived share at or below 100%.
    anyLast(JSONExtractUInt(plan, 'ExecutionTimeNs')) > 0,
    anyLast(JSONExtractUInt(stage, 'WallClockTimeNs')) <= anyLast(JSONExtractUInt(plan, 'ExecutionTimeNs')),
    anyLast(JSONExtractUInt(plan, 'MaxThreads')) > 0
FROM
(
    SELECT
        toJSONString(query_plan) AS plan,
        arrayFilter(n -> JSONExtractString(n, 'Node Type') = 'ReadFromSystemNumbers',
                    JSONExtractArrayRaw(plan, 'Nodes'))[1] AS source,
        JSONExtractArrayRaw(source, 'Statistics', 'Stages')[1] AS stage
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05047_stats'
);
