SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
-- The JSONCompact statistics block carries a wall-clock time, which no reference can pin.
SET output_format_write_statistics = 0;

DROP TABLE IF EXISTS t_04327;
CREATE TABLE t_04327 (id UInt32, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04327 SELECT number, number * 7 FROM numbers(3);

-- A correlated subquery's own totals are not part of its value, so decorrelation must not turn them
-- into the outer query's totals. JSONCompact makes the absence of the "totals" field observable.

-- A: the reported shape, then its non-correlated twin, which master already answers with no totals.
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0) ORDER BY id FORMAT JSONCompact;
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS) ORDER BY id FORMAT JSONCompact;

-- B: an aggregating outer query keeps working. AggregatingStep drops the totals itself, so deciding
-- from the subquery plan alone would reject this correct query.
SELECT count() FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0);

-- C: same via EXISTS.
SELECT id FROM t_04327 WHERE EXISTS (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0) ORDER BY id FORMAT JSONCompact;

-- D: totals produced by a nested subquery and not consumed by an aggregation.
SELECT id FROM t_04327 WHERE id >= (SELECT x FROM (SELECT 0 AS x GROUP BY 1 WITH TOTALS) AS s WHERE t_04327.val >= 0) ORDER BY id FORMAT JSONCompact;

-- E: the outer query's own extremes must survive. Only totals leak through the decorrelation join,
-- which drops both inputs' extremes. The control prints the same block with no subquery involved.
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0) ORDER BY id FORMAT JSONCompact SETTINGS extremes = 1;
SELECT id FROM t_04327 ORDER BY id FORMAT JSONCompact SETTINGS extremes = 1;

-- F: a nested WITH TOTALS consumed by an enclosing aggregation never leaked and still works.
SELECT id FROM t_04327 WHERE id >= (SELECT max(x) FROM (SELECT number AS x FROM numbers(3) GROUP BY number WITH TOTALS) WHERE t_04327.val >= 0) ORDER BY id;

-- G: WITH ROLLUP produces ordinary rows, not a totals stream.
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH ROLLUP HAVING isNull(val) = 0) ORDER BY id;

-- The same shape without WITH TOTALS is decorrelated normally.
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 HAVING isNull(val) = 0) ORDER BY id;

-- H: the carrier side is recorded where the two inputs are unambiguous and re-derived after any
-- reordering, so neither the join-kind swap nor the in-memory buffer changes the result.
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0) ORDER BY id FORMAT JSONCompact SETTINGS correlated_subqueries_default_join_kind = 'left';
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0) ORDER BY id FORMAT JSONCompact SETTINGS correlated_subqueries_default_join_kind = 'right';
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0) ORDER BY id FORMAT JSONCompact SETTINGS correlated_subqueries_use_in_memory_buffer = 1;
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0) ORDER BY id FORMAT JSONCompact SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

-- I: a correlated TotalsHaving step is still refused by the pre-existing unsupported-step check.
SELECT id FROM t_04327 WHERE EXISTS (SELECT val GROUP BY 1 WITH TOTALS); -- { serverError NOT_IMPLEMENTED }

-- J: a correlated subquery whose plan is serialized for distributed execution still answers
-- correctly. This one has no WITH TOTALS, so it guards the distributed route rather than the fix
-- above. The second query is the oracle: the same result without a distributed plan.
DROP TABLE IF EXISTS u_04327;
CREATE TABLE u_04327 (id UInt32, w Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO u_04327 SELECT number, number * 3 FROM numbers(4);

-- Parallel replicas are pinned off because make_distributed_plan rejects them outright
-- (SUPPORT_IS_DISABLED) and the test runner randomizes them on.
SELECT id, count() FROM t_04327 WHERE id >= (SELECT max(w) FROM u_04327 WHERE u_04327.id = t_04327.id) GROUP BY id ORDER BY id
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_force_shuffle_aggregation = 1,
         enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT id, count() FROM t_04327 WHERE id >= (SELECT max(w) FROM u_04327 WHERE u_04327.id = t_04327.id) GROUP BY id ORDER BY id;

-- A warm query result cache replaces the subquery plan with a step that carries the cached totals and
-- contains no TotalsHavingStep, so a check that recognized step types could be defeated by it.
-- Dropping the carrier input's streams at the join cannot be, because it never inspects steps.
SELECT x FROM (SELECT val AS x FROM t_04327 GROUP BY val WITH TOTALS) ORDER BY x
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0;

SELECT count() > 0 FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%GROUP BY val WITH TOTALS%';

-- Deleting the rows the entry was built from makes the hit observable without reading
-- system.query_log: the cached values can only survive if the subquery is served from the cache.
DELETE FROM t_04327 WHERE 1;

SELECT x FROM (SELECT val AS x FROM t_04327 GROUP BY val WITH TOTALS) ORDER BY x
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, enable_writes_to_query_cache = 0;

-- Control: with reads disabled the same query returns nothing, so the row above is a real hit.
SELECT x FROM (SELECT val AS x FROM t_04327 GROUP BY val WITH TOTALS) ORDER BY x
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, enable_writes_to_query_cache = 0, enable_reads_from_query_cache = 0;

INSERT INTO t_04327 SELECT number, number * 7 FROM numbers(3);

SELECT id FROM t_04327 WHERE id >= (SELECT x FROM (SELECT val AS x FROM t_04327 GROUP BY val WITH TOTALS) AS s WHERE t_04327.val >= 0) ORDER BY id FORMAT JSONCompact
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, enable_writes_to_query_cache = 0;

DROP TABLE u_04327;
DROP TABLE t_04327;
