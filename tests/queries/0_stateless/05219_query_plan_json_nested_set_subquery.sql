-- Tags: no-old-analyzer, no-parallel-replicas

-- A set subquery nested inside another one is linked to its consumer, even though that consumer is
-- a step of the other subquery's plan rather than of the query's own.
--
-- This is the case that made the link an id join rather than a name match. Set subqueries used to
-- be tied to their consumers through the `subqueryN` alias printed in a step's description, but
-- those aliases are numbered per plan, so `subquery1` in the query's plan and `subquery1` in a
-- sub-plan are different sets and matching them by name invents links. Both ends now carry the
-- same id, assigned once where the subquery is created.
--
-- It also pins the ordering the link depends on: the consuming step is created by *optimization*
-- -- the `IN` is pushed down into a `PREWHERE` -- so a capture taken before the plan is optimized
-- records the ids onto steps that are then thrown away, and the nested link silently disappears.

DROP TABLE IF EXISTS t_outer_05219;
DROP TABLE IF EXISTS t_mid_05219;
DROP TABLE IF EXISTS t_inner_05219;

CREATE TABLE t_outer_05219 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_mid_05219   (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_inner_05219 (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_outer_05219 SELECT number FROM numbers(1000);
INSERT INTO t_mid_05219   SELECT number * 2 FROM numbers(500);
INSERT INTO t_inner_05219 SELECT number * 4 FROM numbers(100);

SET log_query_plans = 1;

SELECT count() FROM t_outer_05219
WHERE k IN (SELECT k FROM t_mid_05219 WHERE k IN (SELECT k FROM t_inner_05219))
    SETTINGS log_comment = '05219_nested' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

WITH
    toJSONString(query_plan) AS plan,
    JSONExtractArrayRaw(plan, 'SubPlans') AS subqueries,
    JSONExtractArrayRaw(plan, 'Nodes') AS nodes
SELECT
    length(subqueries) AS sub_plans,
    -- Every subquery names at least one consumer.
    arrayAll(s -> length(JSONExtractArrayRaw(s, 'ConsumedBy')) > 0, subqueries) AS all_linked,
    -- Exactly one of them is consumed from inside another subquery's plan rather than from the
    -- query's own plan. That is the link a per-plan name match could not express.
    countIf(
        arrayExists(n ->
            (JSONExtractString(n, 'Node Id') = consumer) AND JSONHas(n, 'SubPlanId'),
            nodes)) AS consumed_inside_another_sub_plan
FROM
(
    WITH
        toJSONString(query_plan) AS plan,
        JSONExtractArrayRaw(plan, 'SubPlans') AS subqueries
    SELECT
        query_plan,
        arrayJoin(arrayMap(c -> JSONExtractString(c),
            arrayFlatten(arrayMap(s -> JSONExtractArrayRaw(s, 'ConsumedBy'), subqueries)))) AS consumer
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05219_nested'
    ORDER BY event_time DESC
    LIMIT 1 BY query_id
)
GROUP BY plan, subqueries, nodes;

DROP TABLE t_outer_05219;
DROP TABLE t_mid_05219;
DROP TABLE t_inner_05219;
