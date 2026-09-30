-- Tags: no-old-analyzer, no-parallel-replicas

-- A subquery in the `ON` section of an `IE JOIN` names the step that reads it.
--
-- The conjuncts beyond the two inequalities become the residual condition of `IEJoinStep`, which
-- keeps them in an `ExpressionActions` of its own rather than in a `Filter` step above the join.
-- Steps are asked for the `ActionsDAG`s they own, so that residual is reached like any other
-- expression; a step that answered for nothing would capture and time the sub-plan while leaving
-- `ConsumedBy` empty.

DROP TABLE IF EXISTS t_iel_05261;
DROP TABLE IF EXISTS t_ier_05261;
DROP TABLE IF EXISTS t_iek_05261;

CREATE TABLE t_iel_05261 (a Int32, b Int32, k Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_ier_05261 (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_iek_05261 (k Int32) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_iel_05261 SELECT number, number * 2, number % 10 FROM numbers(200);
INSERT INTO t_ier_05261 SELECT number + 5, number * 2 + 5 FROM numbers(200);
INSERT INTO t_iek_05261 VALUES (1), (2), (3);

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
-- Keep the join order optimizer from turning this into a kind that applies the extra conjunct as
-- a filter above the join instead of as the residual inside it.
SET query_plan_optimize_join_order_limit = 0;

SET log_query_plans = 1;

-- `LEFT`, not `INNER`: for `ALL INNER JOIN` the extra conjunct becomes a filter over the join
-- result, and the residual is what this test is about.
SELECT count()
FROM t_iel_05261 LEFT JOIN t_ier_05261
    ON t_iel_05261.a < t_ier_05261.a
   AND t_iel_05261.b < t_ier_05261.b
   AND t_iel_05261.k > (SELECT max(k) FROM t_iek_05261)
    SETTINGS log_comment = '05261_ie_join' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

WITH
    toJSONString(query_plan) AS plan,
    JSONExtractRaw(plan, 'SubPlans', 1) AS sub_plan,
    JSONExtractArrayRaw(plan, 'Nodes') AS nodes
SELECT
    'ie_join',
    length(JSONExtractArrayRaw(plan, 'SubPlans')) AS sub_plans,
    JSONExtractString(sub_plan, 'Kind') AS kind,
    -- By node type rather than by id, which carries a serial number.
    arrayStringConcat(
        arraySort(arrayMap(
            c -> JSONExtractString(
                arrayFilter(n -> JSONExtractString(n, 'Node Id') = JSONExtractString(c), nodes)[1],
                'Node Type'),
            JSONExtractArrayRaw(sub_plan, 'ConsumedBy'))),
        ',') AS consumer_types
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment = '05261_ie_join';

DROP TABLE t_iel_05261;
DROP TABLE t_ier_05261;
DROP TABLE t_iek_05261;
