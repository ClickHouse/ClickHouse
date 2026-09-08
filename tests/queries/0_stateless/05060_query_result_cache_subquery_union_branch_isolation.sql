-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- The Planner-level query result cache read path is banned while a distributed plan is being built
-- (see `05054_query_result_cache_subquery_read_ban_settings_escape`). The ban is keyed to the sticky
-- `SelectQueryOptions::building_distributed_plan`, which the `Planner` raises in the options object
-- it is given. Every branch of a `UNION` has its own context with its own SETTINGS clause, so a
-- branch-local `make_distributed_plan = 1` must not be observed by its siblings: the branches used
-- to share one options object, so a later branch could no longer read from the subquery cache.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_union_isolation;
CREATE TABLE t_qrc_union_isolation (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_union_isolation SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET enable_reads_from_query_cache = 1;
-- `make_distributed_plan` rejects an aggregation with a `max_rows_to_group_by` limit, which some CI
-- profiles set.
SET max_rows_to_group_by = 0;

-- The first branch enables `make_distributed_plan` for itself only; the second branch is a plain
-- local query with a cacheable `IN` subquery, so its subquery must probe the cache.
SELECT sum(k) FROM
(
    SELECT k FROM t_qrc_union_isolation WHERE k < 3 SETTINGS make_distributed_plan = 1
    UNION ALL
    SELECT k FROM t_qrc_union_isolation WHERE k IN (SELECT k FROM t_qrc_union_isolation WHERE k >= 8 SETTINGS use_query_cache = 1)
)
SETTINGS log_comment = '05060_union_branch_isolation';

SYSTEM FLUSH LOGS query_log;
-- The cacheable subquery of the second branch must probe the cache (a hit or a miss, depending on
-- what the cache holds); before the fix the first branch's setting suppressed the probe.
SELECT sum(ProfileEvents['QueryCacheHits'] + ProfileEvents['QueryCacheMisses']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND is_initial_query
    AND log_comment = '05060_union_branch_isolation';

DROP TABLE t_qrc_union_isolation;
SYSTEM DROP QUERY CACHE;
