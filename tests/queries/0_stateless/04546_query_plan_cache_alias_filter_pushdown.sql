SET allow_experimental_query_plan_cache = 1;
SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

CREATE TEMPORARY TABLE test_start (ts DateTime) ENGINE = Memory;
INSERT INTO test_start VALUES (now());

DROP TABLE IF EXISTS t_query_plan_cache_alias_filter_pushdown;
CREATE TABLE t_query_plan_cache_alias_filter_pushdown
(
    id UInt64,
    value String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_query_plan_cache_alias_filter_pushdown
SELECT number, toString(number)
FROM numbers(100);

SELECT value
FROM t_query_plan_cache_alias_filter_pushdown AS src
WHERE src.id = 42
SETTINGS log_comment = 'query_plan_cache_alias_filter_pushdown'
FORMAT Null;

SELECT value
FROM t_query_plan_cache_alias_filter_pushdown AS src
WHERE src.id = 42
SETTINGS log_comment = 'query_plan_cache_alias_filter_pushdown'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['QueryPlanCacheHits'] AS hits,
    ProfileEvents['QueryPlanCacheMisses'] AS misses,
    ProfileEvents['SelectedMarks'] < ProfileEvents['SelectedMarksTotal'] AS used_primary_key
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'query_plan_cache_alias_filter_pushdown'
ORDER BY event_time_microseconds;

DROP TABLE t_query_plan_cache_alias_filter_pushdown;
