-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- `make_distributed_plan = 1` does not mean the plan is really split into fragments:
-- `QueryPlan::convertToDistributed` falls back to a single local stage for a plan that cannot run on
-- a worker but carries no exchange (`Memory` engine below). Nothing is serialized on that path, so an
-- explicit subquery `SETTINGS use_query_cache = 1` must still populate the query result cache.
-- The complementary case - a plan that really is distributed, where the cache write step is dropped
-- before the split - is covered by `04651_query_result_cache_subquery_parallel_replicas_plan_serialization`.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_local_fallback;
CREATE TABLE t_qrc_local_fallback (k UInt64) ENGINE = Memory;
INSERT INTO t_qrc_local_fallback SELECT number FROM numbers(10);

-- Subquery caching is a Planner feature: with the old analyzer no subquery cache entry is created at
-- all, so the check below would trivially return 0. Pin the analyzer, because some CI jobs run the
-- whole suite with `enable_analyzer = 0`.
SET enable_analyzer = 1;

SET make_distributed_plan = 1;
-- No aggregation and no sorting: those become exchange steps, and a plan with an exchange but a
-- non-remote leaf is rejected instead of falling back. A single matching row keeps the result
-- deterministic without an `ORDER BY`.
SELECT k FROM (SELECT k FROM t_qrc_local_fallback WHERE k = 4 SETTINGS use_query_cache = 1);

-- The system-table assertion itself is not part of the distributed-plan test.
-- Leave distributed-plan mode before querying the local system table.
SET make_distributed_plan = 0;
SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%' || currentDatabase() || '.t_qrc_local_fallback WHERE k = 4%';

DROP TABLE t_qrc_local_fallback;
SYSTEM DROP QUERY CACHE;
