-- Tags: no-parallel
-- - no-parallel - global failpoint `prepared_sets_build_ordered_set_inplace_fail` and the shared query result cache

-- Regression test for "Not-ready Set is passed as the second argument" when the `IN` subquery result
-- also goes through the query result cache.
--
-- With `query_cache_for_subqueries = 1` the Planner wraps the subquery plan with
-- `StreamInQueryResultCacheStep` on the write path, and replaces the whole plan with
-- `ReadFromQueryResultCacheStep` on a cache hit. Neither step used to implement `clone`, so
-- `FutureSetFromSubquery::buildOrderedSetInplace` fell back to consuming the canonical `source` plan,
-- and a silent in-place build failure (forced here by the failpoint) left the set permanently unbuilt
-- so `FunctionIn` threw.
--
-- `StreamInQueryResultCacheStep::clone` copies the cache writer (same key, fresh buffer, so the two
-- plans cannot buffer the same rows twice) and `ReadFromQueryResultCacheStep::clone` replays the
-- cached chunks, so both shapes keep the source plan intact and the deferred
-- `DelayedCreatingSetsStep::makePlansForSets` can rebuild the set.

DROP TABLE IF EXISTS t_qrc_in;
CREATE TABLE t_qrc_in (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_in SELECT number FROM numbers(1000);

SET use_index_for_in_with_subqueries = 1;
SET enable_analyzer = 1;
SET use_query_cache = 1;
SET query_cache_for_subqueries = 1;

SYSTEM DROP QUERY CACHE;

-- Write path: the subquery is not cached yet, so its plan ends with `StreamInQueryResultCacheStep`.
-- The failpoint fires once and skips `finishInsert` on the in-place build during primary key
-- analysis; the preserved (cloned) source lets the deferred build recover and produce the correct
-- result. The subquery matches k in [0, 10), so the outer count is 10.
SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() FROM t_qrc_in WHERE k IN (SELECT k FROM t_qrc_in WHERE k < 10);
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

-- Read path: the deferred build above stored the subquery result, so the subquery plan is now a
-- single `ReadFromQueryResultCacheStep`. The outer query is spelled differently on purpose, so that
-- it is a cache miss itself and the set is actually built instead of the whole result being served
-- from the cache.
SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() AS c FROM t_qrc_in WHERE k IN (SELECT k FROM t_qrc_in WHERE k < 10);
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

-- The previous query hit the cache exactly once, for the `IN` subquery: proof that the read path was
-- exercised, not just the write path.
SET use_query_cache = 0;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE 'SELECT count() AS c FROM t_qrc_in%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM DROP QUERY CACHE;
DROP TABLE t_qrc_in;
