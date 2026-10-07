SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS qpc_ctas_source;
DROP TABLE IF EXISTS qpc_ctas_first;
DROP TABLE IF EXISTS qpc_ctas_second;
CREATE TABLE qpc_ctas_source (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO qpc_ctas_source VALUES (1), (2);
CREATE TEMPORARY TABLE qpc_ctas_test_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO qpc_ctas_test_start VALUES (now64(6));

SET log_comment = 'qpc_ctas_first';
CREATE TABLE qpc_ctas_first ENGINE = MergeTree ORDER BY a AS SELECT a FROM qpc_ctas_source;
SET log_comment = 'qpc_ctas_second';
CREATE TABLE qpc_ctas_second ENGINE = MergeTree ORDER BY a AS SELECT a FROM qpc_ctas_source;
SET log_comment = '';

SELECT groupArray(a) FROM qpc_ctas_first;
SELECT groupArray(a) FROM qpc_ctas_second;

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['QueryPlanCacheHits'],
    ProfileEvents['QueryPlanCacheMisses'],
    ProfileEvents['QueryPlanCacheValidationMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND event_time_microseconds >= (SELECT ts FROM qpc_ctas_test_start)
  AND log_comment IN ('qpc_ctas_first', 'qpc_ctas_second')
  AND startsWith(query, 'CREATE TABLE qpc_ctas_')
ORDER BY log_comment;

DROP TABLE qpc_ctas_source;
DROP TABLE qpc_ctas_first;
DROP TABLE qpc_ctas_second;
