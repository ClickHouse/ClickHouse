-- Tags: no-parallel
-- no-parallel: 清空实例级查询计划缓存，避免干扰并发缓存测试。

-- Setting-driven filters (`additional_table_filters`, `additional_result_filter`) are attached by
-- the planner and are invisible to the pre-analysis cache key. They can also inject set subplans
-- reading tables outside the single-table dependency fingerprint of a cache entry, whose schema and
-- access rights are not revalidated on a cache hit. Such queries must bypass the cache, and their
-- results must stay correct - in particular, an unqualified `additional_table_filters` key applies
-- only when the table is in the session current database, even if the query uses a qualified name.

SET enable_query_plan_cache = 1;
SET enable_parallel_replicas = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 VALUES (1), (2);

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603_start;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603_start VALUES (now64(6));

SYSTEM DROP QUERY PLAN CACHE;

USE {CLICKHOUSE_DATABASE:Identifier};
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 ORDER BY a
SETTINGS additional_table_filters = {'t_query_plan_cache_04603': 'a = 1'}, log_comment = 'qpc_04603';
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 ORDER BY a
SETTINGS additional_table_filters = {'t_query_plan_cache_04603': 'a = 1'}, log_comment = 'qpc_04603';

-- Check the opposite cache insertion order with a different query AST.
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 WHERE a > 0 ORDER BY a
SETTINGS additional_table_filters = {'t_query_plan_cache_04603': 'a = 1'}, log_comment = 'qpc_04603';
USE {CLICKHOUSE_DATABASE:Identifier};
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 WHERE a > 0 ORDER BY a
SETTINGS additional_table_filters = {'t_query_plan_cache_04603': 'a = 1'}, log_comment = 'qpc_04603';

-- `additional_result_filter` is rejected as well.
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 ORDER BY a
SETTINGS additional_result_filter = 'a = 2', log_comment = 'qpc_04603';
SELECT a FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603 ORDER BY a
SETTINGS additional_result_filter = 'a = 2', log_comment = 'qpc_04603';

SYSTEM FLUSH LOGS query_log;
SELECT
    sum(ProfileEvents['QueryPlanCacheHits']),
    sum(ProfileEvents['QueryPlanCacheMisses'])
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time_microseconds >= (SELECT ts FROM {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603_start)
  AND log_comment = 'qpc_04603'
  AND current_database IN (currentDatabase(), concat(currentDatabase(), '_1'));

DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.t_query_plan_cache_04603_start;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
