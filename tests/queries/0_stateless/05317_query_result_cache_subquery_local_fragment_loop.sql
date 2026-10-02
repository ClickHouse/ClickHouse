-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- The Planner-level query result cache read path is banned while a distributed plan is being built,
-- except inside the in-process local fragment of a distributed query (see
-- `05059_query_result_cache_subquery_local_fragment_nested`). The `loop` table function plans a
-- synthetic `SELECT ... FROM <inner storage>` of its own (`ReadFromLoopStep`), and it used to start
-- from fresh `SelectQueryOptions`, so the subqueries of the inner storage lost the exemption inside a
-- local fragment, like the body of a `View` did (`05218_query_result_cache_subquery_local_fragment_view`).
-- The fact is now carried through `SelectQueryInfo`.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_local_loop_dist;
DROP VIEW IF EXISTS t_qrc_local_loop_outer;
DROP VIEW IF EXISTS t_qrc_local_loop_inner;
DROP TABLE IF EXISTS t_qrc_local_loop_src;
CREATE TABLE t_qrc_local_loop_src (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_local_loop_src SELECT number FROM numbers(10);
-- The cacheable subquery lives in the view read by `loop`.
CREATE VIEW t_qrc_local_loop_inner AS
    SELECT k FROM t_qrc_local_loop_src WHERE k IN (SELECT k FROM t_qrc_local_loop_src WHERE k < 5 SETTINGS use_query_cache = 1);
-- A `Distributed` table cannot read a table function directly, so wrap `loop` into a view.
CREATE VIEW t_qrc_local_loop_outer AS
    SELECT k FROM loop(currentDatabase(), t_qrc_local_loop_inner) LIMIT 10;
CREATE TABLE t_qrc_local_loop_dist (k UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_qrc_local_loop_outer);

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
-- process by `createLocalPlan`, and each of them reads ten rows of `loop` over the inner view, which
-- returns only the rows 0..4.
SELECT count(), max(k) FROM t_qrc_local_loop_dist
    SETTINGS make_distributed_plan = 1, log_comment = '05317_local_fragment_loop';

SYSTEM FLUSH LOGS query_log;
-- The subquery of the inner view must probe the cache (a hit or a miss, depending on what the cache
-- holds); before the fix no probe happened at all.
SELECT sum(ProfileEvents['QueryCacheHits'] + ProfileEvents['QueryCacheMisses']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND is_initial_query
    AND log_comment = '05317_local_fragment_loop';

DROP TABLE t_qrc_local_loop_dist;
DROP VIEW t_qrc_local_loop_outer;
DROP VIEW t_qrc_local_loop_inner;
DROP TABLE t_qrc_local_loop_src;
SYSTEM DROP QUERY CACHE;
