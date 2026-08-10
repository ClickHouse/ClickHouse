-- Tags: no-parallel
-- Tag no-parallel: resets the global query plan cache and inspects system.query_log.

SET allow_experimental_query_plan_cache = 1;
SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS qpc_dependency_compatible;
DROP TABLE IF EXISTS qpc_dependency_compatible_test_start;
CREATE TABLE qpc_dependency_compatible_test_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO qpc_dependency_compatible_test_start VALUES (now64(6));
CREATE TABLE qpc_dependency_compatible (a UInt64, unused String) ENGINE = MergeTree ORDER BY a;
INSERT INTO qpc_dependency_compatible VALUES (1, 'old');
SYSTEM DROP QUERY PLAN CACHE;

SELECT a FROM qpc_dependency_compatible SETTINGS log_comment = 'qpc_compatible_seed';

ALTER TABLE qpc_dependency_compatible COMMENT COLUMN unused 'an unrelated comment';
SELECT a FROM qpc_dependency_compatible SETTINGS log_comment = 'qpc_compatible_comment';

DROP TABLE qpc_dependency_compatible SYNC;
CREATE TABLE qpc_dependency_compatible (a UInt64, unused FixedString(8)) ENGINE = MergeTree ORDER BY a;
INSERT INTO qpc_dependency_compatible VALUES (7, 'new');
SELECT a FROM qpc_dependency_compatible SETTINGS log_comment = 'qpc_compatible_recreate';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['QueryPlanCacheHits'],
    ProfileEvents['QueryPlanCacheMisses'],
    ProfileEvents['QueryPlanCachePreAnalysisHits'],
    ProfileEvents['QueryPlanCacheValidationMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND event_time_microseconds >= (SELECT ts FROM qpc_dependency_compatible_test_start)
  AND log_comment IN ('qpc_compatible_comment', 'qpc_compatible_recreate')
ORDER BY log_comment;

DROP TABLE qpc_dependency_compatible;
DROP TABLE qpc_dependency_compatible_test_start;
