-- Tags: no-parallel
-- Tag no-parallel: uses global query plan cache and checks the thread usage of queries.

-- `max_threads` is not a part of the query plan cache key. A plan cached by a query with
-- `max_threads = 1` must not limit a later query with a larger `max_threads` to one thread.

SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_concurrency_control = 0;
SET max_threads_min_free_memory_per_thread = 0;

CREATE TEMPORARY TABLE test_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO test_start VALUES (now64(6));

DROP TABLE IF EXISTS qpc_hit_max_threads;

CREATE TABLE qpc_hit_max_threads (a UInt64, v UInt64) ENGINE = MergeTree ORDER BY a;
SYSTEM STOP MERGES qpc_hit_max_threads;
INSERT INTO qpc_hit_max_threads SELECT number, number FROM numbers(1000000);
INSERT INTO qpc_hit_max_threads SELECT number, number FROM numbers(1000000);
INSERT INTO qpc_hit_max_threads SELECT number, number FROM numbers(1000000);
INSERT INTO qpc_hit_max_threads SELECT number, number FROM numbers(1000000);
INSERT INTO qpc_hit_max_threads SELECT number, number FROM numbers(1000000);
INSERT INTO qpc_hit_max_threads SELECT number, number FROM numbers(1000000);
INSERT INTO qpc_hit_max_threads SELECT number, number FROM numbers(1000000);
INSERT INTO qpc_hit_max_threads SELECT number, number FROM numbers(1000000);

SYSTEM DROP QUERY PLAN CACHE;

SELECT a % 1000 AS k, sum(v) FROM qpc_hit_max_threads GROUP BY k FORMAT Null
SETTINGS max_threads = 1, log_comment = 'qpc_hit_max_threads_1_seed';
SELECT a % 1000 AS k, sum(v) FROM qpc_hit_max_threads GROUP BY k FORMAT Null
SETTINGS max_threads = 16, log_comment = 'qpc_hit_max_threads_2_hit';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['QueryPlanCacheHits'] AS hits,
    ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time_microseconds >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND startsWith(log_comment, 'qpc_hit_max_threads_')
ORDER BY log_comment;

-- The query that hits the cache uses clearly more threads than the one that seeded it. Without
-- recomputing the plan-level thread limit, it would use only a few threads more (for example, for
-- merging the aggregation states), but not as many as `max_threads` allows.
SELECT
    maxIf(peak_threads_usage, log_comment = 'qpc_hit_max_threads_2_hit')
        >= maxIf(peak_threads_usage, log_comment = 'qpc_hit_max_threads_1_seed') + 4 AS hit_uses_more_threads
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time_microseconds >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND startsWith(log_comment, 'qpc_hit_max_threads_');

DROP TABLE qpc_hit_max_threads;
