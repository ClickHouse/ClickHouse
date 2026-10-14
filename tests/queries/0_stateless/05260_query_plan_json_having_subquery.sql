-- Tags: no-old-analyzer, no-parallel-replicas

-- A subquery consumed by `HAVING` names the step that read it.
--
-- `HAVING` over an aggregation with `WITH TOTALS` survives as a `TotalsHavingStep`, which owns its
-- own `ActionsDAG` and is neither an `ExpressionStep` nor a `FilterStep`. Every step is therefore
-- asked for the DAGs it owns rather than matched against a list of step types, otherwise the
-- sub-plan is captured and timed while no step lists it in its `SubPlans`, leaving the document
-- showing a subquery that ran with nothing saying which step wanted its value.

DROP TABLE IF EXISTS t_having_05260;
DROP TABLE IF EXISTS t_bound_05260;

CREATE TABLE t_having_05260 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_having_05260 SELECT number % 50, number FROM numbers(10000);

CREATE TABLE t_bound_05260 (b UInt64) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_bound_05260 SELECT number FROM numbers(10);

SET log_query_plans = 1;

SELECT k, sum(v) AS s
FROM t_having_05260
GROUP BY k
    WITH TOTALS
HAVING s > (SELECT max(b) FROM t_bound_05260)
ORDER BY k
    SETTINGS log_comment = '05260_having' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

WITH
    toJSONString(query_plan) AS plan,
    JSONExtractRaw(plan, 'Plans', 2) AS sub_plan,
    arrayFlatten(arrayMap(p -> JSONExtractArrayRaw(p, 'Nodes'), JSONExtractArrayRaw(plan, 'Plans'))) AS nodes
SELECT
    'having',
    length(JSONExtractArrayRaw(plan, 'Plans')) - 1 AS sub_plans,
    JSONExtractString(sub_plan, 'Kind') AS kind,
    -- The step that read the value, by type rather than by id, which carries a serial number.
    arrayStringConcat(
        arraySort(arrayMap(
            n -> JSONExtractString(n, 'Node Type'),
            arrayFilter(n -> has(JSONExtract(n, 'SubPlans', 'Array(UInt64)'), JSONExtractUInt(sub_plan, 'SubqueryId')), nodes))),
        ',') AS consumer_types
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment = '05260_having';

DROP TABLE t_having_05260;
DROP TABLE t_bound_05260;
