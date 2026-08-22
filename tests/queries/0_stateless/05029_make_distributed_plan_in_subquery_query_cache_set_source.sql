-- Tags: no-parallel, no-old-analyzer
-- - no-parallel - the query result cache is shared

-- A cacheable `IN` subquery must still distribute its set source under `make_distributed_plan = 1`.
--
-- With an explicit `SETTINGS use_query_cache = 1` on the subquery (or `query_cache_for_subqueries = 1`)
-- the Planner wraps the subquery plan with `StreamInQueryResultCacheStep`, which is not serializable.
-- `convertSetSourceForDistributedPlan` used to reject the whole source because of it and silently
-- build the set on the initiator, even though `QueryPlan::convertToDistributed` drops the step before
-- the plan is split into fragments. The step is ignored by the serializability gate now, so the set
-- source goes through distributed planning exactly as it does without the cache setting.
--
-- The marker is the query result cache itself: a really distributed source has its cache write step
-- dropped, so it writes no entry, while a source that stayed on the initiator writes one. (The
-- transfer limits are not a marker here: `SetsSerialization` checks them on the shipped values
-- regardless of where the source ran.)

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_set_source_big;
DROP TABLE IF EXISTS t_qrc_set_source_small;
CREATE TABLE t_qrc_set_source_big (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_qrc_set_source_small (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qrc_set_source_big SELECT number FROM numbers(1000);
INSERT INTO t_qrc_set_source_small SELECT number FROM numbers(100);

-- Subquery caching is a Planner feature: with the old analyzer no subquery cache entry is created at
-- all, so the checks below would trivially return 0. Pin the analyzer, because some CI jobs run the
-- whole suite with `enable_analyzer = 0`.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET allow_experimental_correlated_subqueries = 0, rewrite_in_to_join = 0;
SET use_index_for_in_with_subqueries = 1, use_query_condition_cache = 0;

SELECT '-- without a distributed plan the set source is built locally and caches';
SELECT count() FROM t_qrc_set_source_big WHERE k IN (SELECT id FROM t_qrc_set_source_small WHERE id < 50 SETTINGS use_query_cache = 1);
SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%' || currentDatabase() || '.t_qrc_set_source_small WHERE id < 50%';

SYSTEM DROP QUERY CACHE;

SELECT '-- the same set source is distributed with make_distributed_plan, so it caches nothing';
SELECT count() FROM t_qrc_set_source_big WHERE k IN (SELECT id FROM t_qrc_set_source_small WHERE id < 50 SETTINGS use_query_cache = 1)
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;
-- The system-table assertion itself is not part of the distributed-plan test, so it runs without it.
SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%' || currentDatabase() || '.t_qrc_set_source_small WHERE id < 50%';

SYSTEM DROP QUERY CACHE;

SELECT '-- a set source that cannot be distributed still builds locally and caches';
SELECT count() FROM t_qrc_set_source_big WHERE k IN (SELECT number FROM numbers(50) GROUP BY number SETTINGS use_query_cache = 1)
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%numbers(50) GROUP BY number%';

DROP TABLE t_qrc_set_source_big;
DROP TABLE t_qrc_set_source_small;
SYSTEM DROP QUERY CACHE;
