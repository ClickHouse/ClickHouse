-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- The Planner-level query result cache read path is banned while a distributed plan is being built,
-- except inside the in-process local fragment of a distributed query (see
-- `05059_query_result_cache_subquery_local_fragment_nested`). That fact travels down the planning
-- recursion in `SelectQueryOptions`, but a fragment that reads through a `View` interprets the view
-- body with fresh options (`StorageView::readImpl`), so the cacheable subqueries of the view body
-- used to lose the exemption: `make_distributed_plan` re-raised the ban from the context while both
-- local-fragment flags stayed clear. The fact is now carried through `SelectQueryInfo`.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_local_view_dist;
DROP VIEW IF EXISTS t_qrc_local_view;
DROP TABLE IF EXISTS t_qrc_local_view_src;
CREATE TABLE t_qrc_local_view_src (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_local_view_src SELECT number FROM numbers(10);
-- The cacheable subquery lives in the view body, not in the outer query.
CREATE VIEW t_qrc_local_view AS
    SELECT k FROM t_qrc_local_view_src WHERE k IN (SELECT k FROM t_qrc_local_view_src WHERE k < 5 SETTINGS use_query_cache = 1);
CREATE TABLE t_qrc_local_view_dist (k UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_qrc_local_view);

-- Subquery caching is a Planner feature: with the old analyzer no cache probe happens at all.
SET enable_analyzer = 1;
SET enable_reads_from_query_cache = 1;
-- `make_distributed_plan` rejects an aggregation with a `max_rows_to_group_by` limit, which some CI
-- profiles set.
SET max_rows_to_group_by = 0;
-- The shard plans must be built in this process: the randomized `prefer_localhost_replica = 0` sends
-- them to `localhost:9000` over the network instead, where `make_distributed_plan` refuses the query
-- outright (`SUPPORT_IS_DISABLED`).
SET prefer_localhost_replica = 1;

-- Both shards of `test_cluster_two_shards_localhost` are local, so the shard plans are built in this
-- process by `createLocalPlan`, and each of them reads the view. 0 + 1 + ... + 4 = 10, twice.
SELECT sum(k) FROM t_qrc_local_view_dist
    SETTINGS make_distributed_plan = 1, log_comment = '05218_local_fragment_view';

SYSTEM FLUSH LOGS query_log;
-- The subquery of the view body must probe the cache (a hit or a miss, depending on what the cache
-- holds); before the fix no probe happened at all.
SELECT sum(ProfileEvents['QueryCacheHits'] + ProfileEvents['QueryCacheMisses']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND is_initial_query
    AND log_comment = '05218_local_fragment_view';

DROP TABLE t_qrc_local_view_dist;
DROP VIEW t_qrc_local_view;
DROP TABLE t_qrc_local_view_src;
SYSTEM DROP QUERY CACHE;
