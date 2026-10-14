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
    arraySlice(JSONExtractArrayRaw(plan, 'Plans'), 2) AS subqueries,
    arrayFlatten(arrayMap(p -> JSONExtractArrayRaw(p, 'Nodes'), JSONExtractArrayRaw(plan, 'Plans'))) AS all_nodes
SELECT
    length(subqueries) AS sub_plans,
    -- Every subquery is listed in the `SubPlans` of at least one consumer.
    arrayAll(s -> arrayExists(
        n -> has(JSONExtract(n, 'SubPlans', 'Array(UInt64)'), JSONExtractUInt(s, 'SubqueryId')),
        all_nodes), subqueries) AS all_linked,
    -- Exactly one of them is consumed from inside another subquery's plan rather than from the
    -- query's own plan: a step of that subquery's plan lists it in `SubPlans`. That is the link a
    -- per-plan name match could not express.
    length(arrayDistinct(arrayFlatten(arrayMap(
        n -> JSONExtract(n, 'SubPlans', 'Array(UInt64)'),
        arrayFlatten(arrayMap(s -> JSONExtractArrayRaw(s, 'Nodes'), subqueries))))))
        AS consumed_inside_another_sub_plan
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05219_nested'
ORDER BY event_time DESC
LIMIT 1 BY query_id;

DROP TABLE t_outer_05219;
DROP TABLE t_mid_05219;
DROP TABLE t_inner_05219;
