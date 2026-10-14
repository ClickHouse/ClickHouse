-- Tags: no-old-analyzer, no-parallel-replicas

-- A scalar subquery is captured, so the stored plan names the table it read.
--
-- `(SELECT ...)` yielding one value is executed during *analysis* and its result folded into the
-- outer query as a literal. Nothing of it survives into the plan -- not the subquery, not even a
-- reference to it -- so before this the document for a query whose scalar subquery scanned a large
-- table described a plan reading a single row from `system.one`, while `read_rows` said otherwise.
--
-- The subquery is still linked to the step that uses its value. A set leaves a `ColumnSet` in the
-- consuming step's actions to find, but a folded literal leaves nothing, so the id is written onto
-- the constant as the planner builds the actions (`ActionsDAG::Node::scalar_subquery_id`) and read
-- back off it afterwards.
--
-- The second half pins the contrasting case. A *correlated* subquery cannot run standalone, so it
-- is decorrelated into the outer plan and executed as ordinary steps -- see the guards in
-- `FutureSetFromSubquery::buildSetInplace`. It is already fully described by the plan, and must
-- therefore produce no sub-plan at all.

DROP TABLE IF EXISTS t_scalar_05220;

CREATE TABLE t_scalar_05220 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_scalar_05220 SELECT number, number * 2 FROM numbers(10000);

SET log_query_plans = 1;

-- Used in a WHERE so there is a step that reads the folded value, and lists it in its `SubPlans`.
SELECT count() FROM t_scalar_05220 WHERE v > (SELECT avg(v) FROM t_scalar_05220 WHERE k > 10)
    SETTINGS log_comment = '05220_scalar' FORMAT Null;

-- The same in the SELECT list. Worth its own case: a constant projected under an alias is rebuilt
-- as a fresh column under the alias's name, so the node the planner marked is gone by the time the
-- plan is serialized -- which is why the id is recorded on the actions as well as on the node.
SELECT (SELECT avg(v) FROM t_scalar_05220 WHERE k > 10) AS s
    SETTINGS log_comment = '05220_projected' FORMAT Null;

SELECT count() FROM t_scalar_05220 AS o
WHERE v = (SELECT max(v) FROM t_scalar_05220 AS i WHERE i.k = o.k)
    SETTINGS log_comment = '05220_correlated' FORMAT Null;

-- Two subqueries folded into one constant. Constant folding collapses the whole expression, so
-- the ids have to travel onto the constant it produces -- and there are two of them, which is why
-- a constant carries a list rather than one id.
SELECT (SELECT sum(v) FROM t_scalar_05220) + (SELECT count() FROM t_scalar_05220) AS s
    SETTINGS log_comment = '05220_folded' FORMAT Null;

-- A subquery supplying LIMIT or OFFSET. The value is consumed by a step that holds no
-- `ActionsDAG` -- `LimitStep` keeps its bound as a plain field -- so it cannot be linked the way
-- every other consumer is, and the ids are put on the step directly instead. Both subqueries here
-- land on the same `Limit` step, since one step carries both bounds.
SELECT k FROM t_scalar_05220 ORDER BY k
    LIMIT (SELECT count() FROM t_scalar_05220 WHERE k < 3)
    OFFSET (SELECT count() FROM t_scalar_05220 WHERE k < 2)
    SETTINGS log_comment = '05220_limit_offset' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

WITH
    toJSONString(query_plan) AS plan,
    arraySlice(JSONExtractArrayRaw(plan, 'Plans'), 2) AS subqueries,
    JSONExtractArrayRaw(plan, 'Plans', 1, 'Nodes') AS main_nodes,
    arrayFlatten(arrayMap(q -> JSONExtractArrayRaw(q, 'Nodes'), subqueries)) AS sub_nodes
SELECT
    replaceOne(log_comment, '05220_', '') AS shape,
    length(subqueries) AS sub_plans,
    JSONExtractString(subqueries[1], 'Kind') AS kind,
    -- The table the subquery read is named. This is the whole point: the outer plan reads only
    -- `system.one`, so without the capture no scan appeared anywhere.
    countSubstrings(plan, concat('"Description":"', currentDatabase(), '.t_scalar_05220"')) > 0 AS names_table,
    -- Its nodes are in the sub-plan's own `Nodes` rather than the query plan's, and carry statistics.
    notEmpty(sub_nodes) AS marked,
    arrayAll(n -> JSONHas(n, 'Statistics'), sub_nodes) AS sub_nodes_have_statistics,
    -- The step that reads the folded value lists it in its `SubPlans`, and it belongs to the query's
    -- own plan.
    arrayExists(
        n -> has(JSONExtract(n, 'SubPlans', 'Array(UInt64)'), JSONExtractUInt(subqueries[1], 'SubqueryId')),
        main_nodes) AS consumer_in_main_plan,
    -- Every captured subquery has a consumer, the folded pair included.
    arrayAll(q -> arrayExists(
        n -> has(JSONExtract(n, 'SubPlans', 'Array(UInt64)'), JSONExtractUInt(q, 'SubqueryId')),
        arrayConcat(main_nodes, sub_nodes)), subqueries) AS all_linked
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05220_scalar', '05220_correlated', '05220_projected', '05220_folded',
                        '05220_limit_offset')
ORDER BY shape;

DROP TABLE t_scalar_05220;
