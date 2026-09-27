-- Regression test for the query-plan serialization version of a `ConstantJoin` step (a `CROSS` /
-- `COMMA` join, or a join with a constant `ON` predicate) whose `max_memory_usage` comes from a
-- subquery-local `SETTINGS` rather than from the query itself.
--
-- `ConstantJoin` consumes `max_memory_usage` as the trigger of its own compaction, so a step-local
-- value must travel with the step: the planner marks such a step (`JoinSettings::fromContext`
-- compares the planning scope against the query context) and
-- `QueryPlanSerializationSettings::getMinRequiredVersion` raises the fragment to version 5, the
-- first version that carries `max_memory_usage` on the wire. The `CROSS` / `COMMA` builder in
-- `PlannerJoinTree` used to construct its `JoinSettings` straight from the settings reference, so the
-- override looked query-wide, the fragment stayed below version 5, and the receiver rebuilt the join
-- with the outer query limit.
--
-- Serializing the plan for a remote receiver is where the version matters, hence
-- `serialize_query_plan = 1` with `prefer_localhost_replica = 0`.

SET enable_analyzer = 1;
SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;
SET enable_join_in_memory_compression = 1;

-- A `CROSS JOIN` under a subquery-local `max_memory_usage`: the plan must serialize and execute.
SELECT count(), sum(l.number + r.number)
FROM
(
    SELECT number FROM remote('127.0.0.1', system.numbers) LIMIT 100
) AS l
CROSS JOIN
(
    SELECT number FROM remote('127.0.0.1', system.numbers) LIMIT 20
) AS r
SETTINGS max_memory_usage = 20000000000;

-- The same with a `COMMA` join and with the override on the inner subquery only, so the step-local
-- limit differs from the query-wide one.
SELECT count()
FROM
(
    SELECT number FROM remote('127.0.0.1', system.numbers) LIMIT 30 SETTINGS max_memory_usage = 15000000000
) AS l,
(
    SELECT number FROM remote('127.0.0.1', system.numbers) LIMIT 7
) AS r;

-- A join with a constant `ON` predicate also executes as `ConstantJoin`.
SELECT count()
FROM
(
    SELECT number FROM remote('127.0.0.1', system.numbers) LIMIT 10
) AS l
INNER JOIN
(
    SELECT number FROM remote('127.0.0.1', system.numbers) LIMIT 4
) AS r ON 1
SETTINGS max_memory_usage = 18000000000;
