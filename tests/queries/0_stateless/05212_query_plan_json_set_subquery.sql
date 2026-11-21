-- Tags: no-old-analyzer

-- A set built during planning is captured, with the statistics its own pipeline produced.
--
-- `k IN (SELECT ...)` on the primary key is run by `FutureSetFromSubquery::buildOrderedSetInplace`
-- during index analysis, before the query's own pipeline exists, and nothing links the subquery's
-- plan into the query's plan tree. Without `SetSubPlanCapture` the stored plan would say the query
-- read every row of `source` and never name the table -- the reason this exists.

DROP TABLE IF EXISTS target;
DROP TABLE IF EXISTS source;

CREATE TABLE target (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE source (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO target SELECT number FROM numbers(10000);
INSERT INTO source SELECT number * 1000 FROM numbers(10);

SET log_query_plans = 1;

SELECT count() FROM target WHERE k IN (SELECT k FROM source)
    SETTINGS log_comment = '05212_set_subquery' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    -- The subquery's plan is in the document as a plan of its own, after the query's, with its own
    -- root so a reader can tell it apart from the query's root.
    length(JSONExtractArrayRaw(toJSONString(query_plan), 'Plans')) - 1 AS sub_plans,
    -- The table the subquery reads is named. This is the whole point: the main plan reads only
    -- `target`, so before the capture no `source` scan appeared anywhere. It is named in the
    -- subquery's own `Nodes`, not in the query plan's.
    countSubstrings(JSONExtractRaw(toJSONString(query_plan), 'Plans', 2, 'Nodes'), concat('"Description":"', currentDatabase(), '.source"')) > 0
        AND countSubstrings(JSONExtractRaw(toJSONString(query_plan), 'Plans', 1, 'Nodes'), concat('"Description":"', currentDatabase(), '.source"')) = 0 AS names_source,
    -- The plan its nodes live in is marked with where it came from.
    JSONExtractString(toJSONString(query_plan), 'Plans', 2, 'Kind') = 'Set' AS marked,
    -- And they carry what their own pipeline measured, not zeros and not the main pipeline's.
    JSONExtractUInt(toJSONString(query_plan), 'Plans', 2, 'ExecutionTimeNs') > 0 AS sub_execution_time,
    JSONExtractUInt(toJSONString(query_plan), 'Plans', 2, 'MaxThreads') > 0 AS sub_max_threads
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05212_set_subquery';

-- Every node of the subquery's plan has statistics, exactly as the main plan's nodes do.
SELECT
    countIf(node.1 = 1 AND node.2 = 1) = countIf(node.1 = 1) AS every_sub_node_has_statistics,
    countIf(node.1 = 1) > 1 AS more_than_one_sub_node
FROM
(
    -- Every node of every plan, paired with whether its plan is a sub-plan: anything after the first.
    WITH JSONExtractArrayRaw(toJSONString(query_plan), 'Plans') AS plans
    SELECT arrayJoin(arrayFlatten(arrayMap((p, i) -> arrayMap(n ->
        (toUInt8(i > 1), toUInt8(JSONHas(n, 'Statistics'))),
        JSONExtractArrayRaw(p, 'Nodes')), plans, arrayEnumerate(plans)))) AS node
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05212_set_subquery'
);

-- The subquery is tied to the step that uses the set it builds, so a reader can see what the query
-- wanted it for. Both ends carry the same assigned id -- the subquery's `SubqueryId` and the
-- consuming step's `SubPlans` -- so the link is a join and holds across plan boundaries.
WITH
    toJSONString(query_plan) AS plan,
    JSONExtractArrayRaw(plan, 'Plans')[2] AS subquery,
    JSONExtractUInt(subquery, 'SubqueryId') AS subquery_id,
    arrayFlatten(arrayMap(p -> JSONExtractArrayRaw(p, 'Nodes'), JSONExtractArrayRaw(plan, 'Plans'))) AS all_nodes,
    arrayFilter(n -> has(JSONExtract(n, 'SubPlans', 'Array(UInt64)'), subquery_id), all_nodes) AS consumers
SELECT
    JSONHas(subquery, 'SubqueryId') AS has_id,
    length(consumers) > 0 AS names_a_consumer,
    -- The consumer is a step of the query's own plan, not one of the subquery's.
    arrayExists(
        n -> has(JSONExtract(n, 'SubPlans', 'Array(UInt64)'), subquery_id),
        JSONExtractArrayRaw(plan, 'Plans', 1, 'Nodes')) AS consumer_is_in_main_plan
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05212_set_subquery';

DROP TABLE target;
DROP TABLE source;
