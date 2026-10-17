-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- Regression test for the interaction of `query_cache_min_query_duration` with the cloned
-- `StreamInQueryResultCacheStep`.
--
-- `FutureSetFromSubquery::buildOrderedSetInplace` clones the `IN` subquery plan and runs the clone, so
-- the *copy* of the `QueryResultCacheWriter` is the only one that is ever finalized - the canonical
-- source plan is dropped once the speculative build succeeded. `min_query_runtime` is measured from
-- the writer's construction, so the copy has to inherit the original's start time. Otherwise the timer
-- restarts at the clone point, the measured runtime is shorter than the real one, and a subquery stops
-- populating the cache just because its plan shape became clonable.
--
-- The first `IN` subquery sleeps for 2 seconds while its set is built in place, which is the elapsed
-- time the second subquery's writer must still account for when it is finalized right afterwards.

DROP TABLE IF EXISTS t_qrc_dur;
CREATE TABLE t_qrc_dur (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_dur SELECT number FROM numbers(1000);

SET enable_analyzer = 1;
SET use_index_for_in_with_subqueries = 1;
SET use_query_cache = 1;
SET query_cache_for_subqueries = 1;
SET query_cache_min_query_duration = 1000;
-- The sleeping subquery must not be cached, and it must not make the query fail either.
SET query_cache_nondeterministic_function_handling = 'ignore';

SYSTEM DROP QUERY CACHE;

-- The first set is {0}, the second one is [0, 10), so the intersection has exactly one element.
SELECT count() FROM t_qrc_dur
WHERE k IN (SELECT number FROM numbers(1) WHERE NOT ignore(sleep(2)))
  AND k IN (SELECT k FROM t_qrc_dur WHERE k < 10);

-- The second subquery ran for well over `query_cache_min_query_duration`, so it is in the cache.
SET use_query_cache = 0;
SELECT count() FROM system.query_cache
WHERE is_subquery = 1 AND query LIKE '%' || currentDatabase() || '.t_qrc_dur WHERE k < 10%';

SYSTEM DROP QUERY CACHE;
DROP TABLE t_qrc_dur;
