-- Tags: no-parallel
-- Tag no-parallel: uses global query plan cache and system.query_log.

SET allow_experimental_query_plan_cache = 1;
SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

CREATE TEMPORARY TABLE test_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO test_start VALUES (now64(6));

DROP ROW POLICY IF EXISTS qpc_pre_analysis_policy_1 ON qpc_pre_analysis;
DROP ROW POLICY IF EXISTS qpc_pre_analysis_policy_2 ON qpc_pre_analysis;
DROP TABLE IF EXISTS qpc_pre_analysis;

CREATE TABLE qpc_pre_analysis (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO qpc_pre_analysis VALUES (1, 'a'), (2, 'b');

SYSTEM DROP QUERY PLAN CACHE;

SELECT id FROM qpc_pre_analysis WHERE id = 1 SETTINGS log_comment = 'qpc_pre_analysis_basic_first' FORMAT Null;
SELECT id FROM qpc_pre_analysis WHERE id = 1 SETTINGS log_comment = 'qpc_pre_analysis_basic_second' FORMAT Null;

SYSTEM DROP QUERY PLAN CACHE;
CREATE ROW POLICY qpc_pre_analysis_policy_1 ON qpc_pre_analysis USING id = 1 TO ALL;

SELECT id FROM qpc_pre_analysis WHERE id = 1 SETTINGS log_comment = 'qpc_pre_analysis_policy_seed' FORMAT Null;

DROP ROW POLICY qpc_pre_analysis_policy_1 ON qpc_pre_analysis;
CREATE ROW POLICY qpc_pre_analysis_policy_2 ON qpc_pre_analysis USING id = 1 TO ALL;

SELECT id FROM qpc_pre_analysis WHERE id = 1 SETTINGS log_comment = 'qpc_pre_analysis_policy_validation_miss' FORMAT Null;
SELECT id FROM qpc_pre_analysis WHERE id = 1 SETTINGS log_comment = 'qpc_pre_analysis_policy_hit' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['QueryPlanCachePreAnalysisHits'] AS pre_analysis_hits,
    ProfileEvents['QueryPlanCacheValidationMisses'] AS validation_misses,
    notEmpty(query_columns) AS has_query_columns,
    arrayExists(policy -> position(policy, 'qpc_pre_analysis_policy_2') > 0, used_row_policies) AS has_current_row_policy
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time_microseconds >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND startsWith(log_comment, 'qpc_pre_analysis_')
ORDER BY log_comment;

DROP ROW POLICY qpc_pre_analysis_policy_2 ON qpc_pre_analysis;
DROP TABLE qpc_pre_analysis;
