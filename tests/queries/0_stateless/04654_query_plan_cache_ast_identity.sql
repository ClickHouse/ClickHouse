-- Tags: no-parallel
-- Tag no-parallel: resets the global query plan cache and inspects system.query_log.

SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS qpc_ast_identity;
DROP TABLE IF EXISTS qpc_ast_identity_test_start;
CREATE TABLE qpc_ast_identity_test_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO qpc_ast_identity_test_start VALUES (now64(6));
CREATE TABLE qpc_ast_identity (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO qpc_ast_identity VALUES (1), (2);
SYSTEM DROP QUERY PLAN CACHE;

SELECT a FROM qpc_ast_identity WHERE a = 1 SETTINGS log_comment = 'qpc_ast_seed';
select a from qpc_ast_identity where a=1 settings log_comment='qpc_ast_canonical_second';

SELECT a AS x FROM qpc_ast_identity WHERE a = 1 SETTINGS log_comment = 'qpc_ast_alias_x';
SELECT a AS y FROM qpc_ast_identity WHERE a = 1 SETTINGS log_comment = 'qpc_ast_alias_y_first';
SELECT a AS y FROM qpc_ast_identity WHERE a = 1 SETTINGS log_comment = 'qpc_ast_alias_y_second';
SELECT a FROM qpc_ast_identity WHERE a = 2 SETTINGS log_comment = 'qpc_ast_literal_first';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['QueryPlanCacheHits'],
    ProfileEvents['QueryPlanCacheMisses'],
    ProfileEvents['QueryPlanCacheValidationMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND event_time_microseconds >= (SELECT ts FROM qpc_ast_identity_test_start)
  AND log_comment IN (
      'qpc_ast_canonical_second',
      'qpc_ast_alias_y_first',
      'qpc_ast_alias_y_second',
      'qpc_ast_literal_first')
ORDER BY log_comment;

DROP TABLE qpc_ast_identity;
DROP TABLE qpc_ast_identity_test_start;
