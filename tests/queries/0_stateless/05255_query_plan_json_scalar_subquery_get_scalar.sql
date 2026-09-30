-- Tags: no-old-analyzer, no-parallel-replicas

-- A scalar subquery whose value is too large to fold into the query is still linked to the step
-- that reads it.
--
-- `evaluateScalarSubqueryIfNeeded` folds a single scalar value in as a literal, but for an
-- `Array`, `Tuple` or `LowCardinality` result it leaves a `__getScalar('<hash>')` call behind and
-- puts the value in the query context. The id that ties the captured sub-plan to its consumer
-- therefore cannot travel on the folded value, because there is none; it travels on the hash the
-- call reads back, which is the only part of the subquery left in the query.
--
-- Without that the sub-plan is captured and timed but `ConsumedBy` is empty, so the document shows
-- a subquery that scanned a table and nothing saying which step wanted it.

DROP TABLE IF EXISTS t_get_scalar_05255;

CREATE TABLE t_get_scalar_05255 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_get_scalar_05255 SELECT number, number * 2 FROM numbers(1000);

SET log_query_plans = 1;

-- An `Array` result: kept as `__getScalar`, not folded.
SELECT length((SELECT groupArray(v) FROM t_get_scalar_05255)) AS n
    SETTINGS log_comment = '05255_array' FORMAT Null;

-- A `Tuple` result, kept the same way.
SELECT (SELECT (count(), max(v)) FROM t_get_scalar_05255) AS t
    SETTINGS log_comment = '05255_tuple' FORMAT Null;

-- The contrasting case: a single value is folded in as a literal and travels on the constant.
-- Both carriers have to end up linked, which is what makes all the rows below identical.
SELECT (SELECT count() FROM t_get_scalar_05255) AS c
    SETTINGS log_comment = '05255_folded' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

WITH
    toJSONString(query_plan) AS plan,
    JSONExtractArrayRaw(plan, 'SubPlans') AS subqueries,
    JSONExtractArrayRaw(plan, 'Nodes') AS nodes
SELECT
    replaceOne(log_comment, '05255_', '') AS shape,
    length(subqueries) AS sub_plans,
    JSONExtractString(subqueries[1], 'Kind') AS kind,
    -- The point of the test: the subquery names the step that reads its value.
    length(JSONExtractArrayRaw(subqueries[1], 'ConsumedBy')) > 0 AS linked,
    -- And every step it names is a step of this document.
    arrayAll(
        c -> arrayExists(n -> JSONExtractString(n, 'Node Id') = JSONExtractString(c), nodes),
        JSONExtractArrayRaw(subqueries[1], 'ConsumedBy')) AS readers_exist
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05255_array', '05255_tuple', '05255_folded')
ORDER BY shape;

DROP TABLE t_get_scalar_05255;
