-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

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

-- The carrier side is re-derived after join-order reconstruction, so forcing the optimizer to flip the
-- join's children must not move the result. Transporting the recorded side without re-deriving it makes
-- this row leak a totals block, because the drop is then applied to the outer input. The functional
-- runner never injects query_plan_join_swap_table = true, but the stress runner does, so pin it here.
-- The order limit is pinned too: at 0 optimizeJoinLogicalImpl returns before the flip can happen, and
-- the runner draws 0 with 5% probability, which would silently make this row a duplicate of the one above.
-- join_algorithm is pinned for the same reason: the decorrelation join is ANY OUTER, partial merge
-- supports only one of its two orientations, and any algorithm list that can still select partial merge
-- (including 'auto', which the stress runner injects) makes chooseJoinOrder cancel the flip. The join
-- kind then stays as planned and this row silently stops exercising the reversal.
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0) ORDER BY id FORMAT JSONCompact
SETTINGS query_plan_join_swap_table = true, query_plan_optimize_join_order_limit = 10, correlated_subqueries_use_in_memory_buffer = 0,
         join_algorithm = 'hash';

-- I: a correlated TotalsHaving step is still refused by the pre-existing unsupported-step check.
SELECT id FROM t_04327 WHERE EXISTS (SELECT val GROUP BY 1 WITH TOTALS); -- { serverError NOT_IMPLEMENTED }

-- J: a correlated subquery whose plan is serialized for distributed execution still answers
-- correctly. This one has no WITH TOTALS, so it guards the distributed route rather than the fix
-- above. The second query is the oracle: the same result without a distributed plan.
DROP TABLE IF EXISTS u_04327;
CREATE TABLE u_04327 (id UInt32, w Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO u_04327 SELECT number, number * 3 FROM numbers(4);

-- make_distributed_plan rejects two things this shape would otherwise hit, both with
-- SUPPORT_IS_DISABLED, so both are pinned: parallel replicas, which the test runner randomizes on,
-- and a non-zero max_rows_to_group_by, which arrives from the ambient limits profile rather than
-- from randomization (tests/config/users.d/limits.yaml sets 10G and install.sh links it for every
-- lane, so --no-random-settings does not help).
SELECT id, count() FROM t_04327 WHERE id >= (SELECT max(w) FROM u_04327 WHERE u_04327.id = t_04327.id) GROUP BY id ORDER BY id
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_force_shuffle_aggregation = 1,
         enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0;

SELECT id, count() FROM t_04327 WHERE id >= (SELECT max(w) FROM u_04327 WHERE u_04327.id = t_04327.id) GROUP BY id ORDER BY id;

-- Without an oracle the row above would still match its reference if make_distributed_plan silently
-- declined this shape, so it would stop guarding the route. Exchange steps exist only in a
-- distributed plan, and a step name is printed by every EXPLAIN mode regardless of
-- explain_query_plan_default, so their presence discriminates: some with the setting, none without.
SELECT countIf(explain ILIKE '%Exchange%') > 0 FROM (
    EXPLAIN SELECT id, count() FROM t_04327 WHERE id >= (SELECT max(w) FROM u_04327 WHERE u_04327.id = t_04327.id) GROUP BY id ORDER BY id
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_force_shuffle_aggregation = 1,
             enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0);

SELECT countIf(explain ILIKE '%Exchange%') > 0 FROM (
    EXPLAIN SELECT id, count() FROM t_04327 WHERE id >= (SELECT max(w) FROM u_04327 WHERE u_04327.id = t_04327.id) GROUP BY id ORDER BY id);

-- A warm query result cache replaces the subquery plan with a step that carries the cached totals and
-- contains no TotalsHavingStep, so a check that recognized step types could be defeated by it.
-- Dropping the carrier input's streams at the join cannot be, because it never inspects steps.

-- The cache below is a server-wide resource whose policy declines a write outright when the cache is
-- full of live entries, which would make the rows that assert the warm entry and the hit fail. Only an
-- unscoped drop frees that capacity: a per-tag drop removes entries carrying our tag, and capacity is
-- a property of the one server-global cache that no tag, key or database can scope. Dropping every
-- entry is why this test is no-parallel. query_cache_tag stays because it keys our entry distinctly
-- and makes the system.query_cache assertion below specific.
SYSTEM DROP QUERY CACHE;

SELECT x FROM (SELECT val AS x FROM t_04327 GROUP BY val WITH TOTALS) ORDER BY x
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0,
         query_cache_tag = '04327_totals';

SELECT count() > 0 FROM system.query_cache WHERE is_subquery = 1 AND tag = '04327_totals' AND query LIKE '%GROUP BY val WITH TOTALS%';

-- Deleting the rows the entry was built from makes the hit observable without reading
-- system.query_log: the cached values can only survive if the subquery is served from the cache.
-- The delete must be visible before the next read, so do not rely on the default of
-- lightweight_deletes_sync.
DELETE FROM t_04327 WHERE 1 SETTINGS lightweight_deletes_sync = 2;

SELECT x FROM (SELECT val AS x FROM t_04327 GROUP BY val WITH TOTALS) ORDER BY x
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, enable_writes_to_query_cache = 0,
         query_cache_tag = '04327_totals';

-- Control: with reads disabled the same query returns nothing, so the row above is a real hit.
SELECT x FROM (SELECT val AS x FROM t_04327 GROUP BY val WITH TOTALS) ORDER BY x
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, enable_writes_to_query_cache = 0, enable_reads_from_query_cache = 0,
         query_cache_tag = '04327_totals';

INSERT INTO t_04327 SELECT number, number * 7 FROM numbers(3);

SELECT id FROM t_04327 WHERE id >= (SELECT x FROM (SELECT val AS x FROM t_04327 GROUP BY val WITH TOTALS) AS s WHERE t_04327.val >= 0) ORDER BY id FORMAT JSONCompact
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, enable_writes_to_query_cache = 0,
         query_cache_tag = '04327_totals', log_comment = '04327_cache_correlated';

-- The rows were restored above, so the statement returns the same main rows whether or not the
-- subquery came from the cache, and a miss would build a real TotalsHaving step that the carrier-side
-- drop handles anyway. Assert the hit directly, otherwise that row asserts nothing about cached
-- totals. log_comment is ignored in cache lookups, so identifying the statement with it is free.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'] > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '04327_cache_correlated'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Leave the shared cache as this test found it.
SYSTEM DROP QUERY CACHE;

DROP TABLE u_04327;
DROP TABLE t_04327;
