SET allow_experimental_query_plan_cache = 1;
SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

CREATE TEMPORARY TABLE test_start (ts DateTime) ENGINE = Memory;
INSERT INTO test_start VALUES (now());

DROP TABLE IF EXISTS t_query_plan_cache_reject_in_table;
DROP TABLE IF EXISTS t_query_plan_cache_reject_in_table_ids;

CREATE TABLE t_query_plan_cache_reject_in_table
(
    id UInt64,
    value String
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_query_plan_cache_reject_in_table_ids
(
    id UInt64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_query_plan_cache_reject_in_table
SELECT number, toString(number)
FROM numbers(10);

INSERT INTO t_query_plan_cache_reject_in_table_ids VALUES (1), (3), (5);

SELECT count()
FROM t_query_plan_cache_reject_in_table
WHERE id IN t_query_plan_cache_reject_in_table_ids
SETTINGS log_comment = 'query_plan_cache_reject_in_table'
FORMAT Null;

SELECT count()
FROM t_query_plan_cache_reject_in_table
WHERE id IN t_query_plan_cache_reject_in_table_ids
SETTINGS log_comment = 'query_plan_cache_reject_in_table'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    sum(ProfileEvents['QueryPlanCacheHits']) AS hits,
    sum(ProfileEvents['QueryPlanCacheMisses']) AS misses,
    count() AS queries
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'query_plan_cache_reject_in_table';

DROP TABLE t_query_plan_cache_reject_in_table;
DROP TABLE t_query_plan_cache_reject_in_table_ids;
