-- Tags: no-old-analyzer, no-parallel-replicas

-- A scalar subquery written twice is evaluated once, and both readers are linked to it.
--
-- The id that ties a captured sub-plan to the steps using its value is assigned where the subquery
-- runs, which is the cache miss in `evaluateScalarSubqueryIfNeeded`. A second occurrence of the
-- same subquery hits the analyzer's cache: the value is folded in exactly as before, but nothing
-- ran, so nothing was recorded and the step reading it looked as though it consumed nothing. The
-- caches belong to `QueryAnalyzer`, one per query analysis, so a hit always refers to a subquery
-- this query has already run and the profiler has already captured -- the id is remembered beside
-- the cached value and handed back, rather than a second sub-plan being invented.
--
-- The check is the size of `ConsumedBy`, not the names in it: which steps end up holding the value
-- depends on how the query is planned, but there must be one sub-plan and two distinct readers.

DROP TABLE IF EXISTS t_cache_hit_05223;

CREATE TABLE t_cache_hit_05223 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_cache_hit_05223 SELECT number, number * 2 FROM numbers(10000);

SET log_query_plans = 1;

-- The same subquery in the projection and in the WHERE. One of the two is a cache hit, and which
-- one it is depends on the order analysis reaches them -- either way both readers must be named.
SELECT (SELECT avg(v) FROM t_cache_hit_05223) AS a, count()
FROM t_cache_hit_05223
WHERE v > (SELECT avg(v) FROM t_cache_hit_05223)
    SETTINGS log_comment = '05223_twice' FORMAT Null;

-- Two *different* subqueries, for contrast: no cache hit, so two sub-plans rather than one reused.
SELECT count()
FROM t_cache_hit_05223
WHERE v > (SELECT avg(v) FROM t_cache_hit_05223) AND k < (SELECT max(k) FROM t_cache_hit_05223)
    SETTINGS log_comment = '05223_distinct' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

WITH
    toJSONString(query_plan) AS plan,
    JSONExtractArrayRaw(plan, 'SubPlans') AS subqueries,
    JSONExtractArrayRaw(plan, 'Nodes') AS nodes
SELECT
    replaceOne(log_comment, '05223_', '') AS shape,
    length(subqueries) AS sub_plans,
    -- Every reader of a cached value is named, and named once: a step consumes a subquery once
    -- however many times it reaches it.
    arrayMap(q -> length(arrayDistinct(JSONExtractArrayRaw(q, 'ConsumedBy'))), subqueries) AS readers,
    arrayAll(q -> length(JSONExtractArrayRaw(q, 'ConsumedBy'))
                = length(arrayDistinct(JSONExtractArrayRaw(q, 'ConsumedBy'))), subqueries) AS no_duplicate_readers,
    -- A cache hit must not invent a sub-plan of its own: each one here still has a root and is a
    -- scalar capture in its own right.
    arrayAll(q -> JSONHas(q, 'Root') AND JSONExtractString(q, 'Kind') = 'Scalar', subqueries) AS all_scalar_with_root,
    -- And every named reader is a step of this document.
    arrayAll(q -> arrayAll(
        c -> arrayExists(n -> JSONExtractString(n, 'Node Id') = JSONExtractString(c), nodes),
        JSONExtractArrayRaw(q, 'ConsumedBy')), subqueries) AS readers_exist
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05223_twice', '05223_distinct')
ORDER BY shape;

DROP TABLE t_cache_hit_05223;
