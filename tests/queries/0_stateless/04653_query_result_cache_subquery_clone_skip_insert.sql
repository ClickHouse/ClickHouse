-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- Regression test for the early-skip decision of a cloned `QueryResultCacheWriter`.
--
-- `QueryResultCacheWriter` sets `skip_insert` in its constructor when the cache already holds a
-- non-stale entry for the key, so that it does not buffer a result it is going to discard anyway. With
-- `enable_reads_from_query_cache = 0` the read probe is skipped, so this is the only place where an
-- already cached `IN` subquery is recognized.
--
-- `FutureSetFromSubquery::buildOrderedSetInplace` clones the subquery plan and runs the clone, and the
-- canonical source plan is dropped once the speculative build succeeded - so the *copy* of the writer is
-- the only one that ever buffers and is finalized. If the copy did not inherit `skip_insert`, it would
-- buffer the whole subquery result into a throwaway buffer, which is exactly the memory the early skip
-- exists to avoid.

DROP TABLE IF EXISTS t_qrc_skip;
CREATE TABLE t_qrc_skip (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_skip SELECT number FROM numbers(1000);

SET enable_analyzer = 1;
SET use_index_for_in_with_subqueries = 1;
SET use_query_cache = 1;
SET query_cache_for_subqueries = 1;

SYSTEM DROP QUERY CACHE;

-- Populate the cache entry of the `IN` subquery.
SELECT count() FROM t_qrc_skip WHERE k IN (SELECT k FROM t_qrc_skip WHERE k < 100);

-- Now run the same subquery with reads from the cache disabled, so that the subquery plan still ends
-- with `StreamInQueryResultCacheStep` and its writer takes the early-skip branch. The outer query is
-- spelled differently on purpose, so that it is a cache miss and the set is really built.
SET enable_reads_from_query_cache = 0;
SELECT count() AS c FROM t_qrc_skip WHERE k IN (SELECT k FROM t_qrc_skip WHERE k < 100);

-- Nothing of the subquery was buffered: the only rows written by the query above are the single row of
-- the outer query's own result.
SET use_query_cache = 0;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheWrittenRows']
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE 'SELECT count() AS c FROM t_qrc_skip%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM DROP QUERY CACHE;
DROP TABLE t_qrc_skip;
