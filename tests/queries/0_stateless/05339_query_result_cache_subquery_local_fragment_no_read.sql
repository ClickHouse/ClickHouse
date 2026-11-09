-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- The Planner-level query result cache read path is banned while a distributed plan is being built
-- (see `05054_query_result_cache_subquery_read_ban_settings_escape`), and the in-process local plans
-- of a distributed query are no exception: the local shard plan of a `Distributed` table is united
-- into the enclosing plan, which `make_distributed_plan` splits into fragments, so a cache hit in a
-- subquery of the local shard plan would make the shape of the outer plan depend on cache contents.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_local_no_read_dist;
DROP VIEW IF EXISTS v_qrc_local_no_read;
DROP TABLE IF EXISTS t_qrc_local_no_read;
CREATE TABLE t_qrc_local_no_read (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_local_no_read SELECT number FROM numbers(10);
CREATE VIEW v_qrc_local_no_read AS SELECT k FROM (SELECT k FROM t_qrc_local_no_read WHERE k < 5 SETTINGS use_query_cache = 1);
CREATE TABLE t_qrc_local_no_read_dist (k UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), v_qrc_local_no_read);

-- Subquery caching is a Planner feature: with the old analyzer no cache probe happens at all.
SET enable_analyzer = 1;
SET enable_reads_from_query_cache = 1;
-- `make_distributed_plan` rejects an aggregation with a `max_rows_to_group_by` limit, which some CI
-- profiles set.
SET max_rows_to_group_by = 0;
-- The shard plans must be built in this process: with `prefer_localhost_replica = 0` they are sent to
-- `localhost:9000` over the network instead, where `make_distributed_plan` refuses the query.
SET prefer_localhost_replica = 1;

-- 0 + 1 + ... + 4 = 10, twice (two shards read the same table).
SELECT sum(k) FROM t_qrc_local_no_read_dist SETTINGS make_distributed_plan = 1, log_comment = '05339_local_fragment_no_read';
SELECT sum(k) FROM t_qrc_local_no_read_dist SETTINGS make_distributed_plan = 1, log_comment = '05339_local_fragment_no_read';

SYSTEM FLUSH LOGS query_log;
-- The subquery of the view body in the local shard plans must not probe the cache.
SELECT count(), sum(ProfileEvents['QueryCacheHits'] + ProfileEvents['QueryCacheMisses'])
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND is_initial_query
    AND log_comment = '05339_local_fragment_no_read';

DROP TABLE t_qrc_local_no_read_dist;
DROP VIEW v_qrc_local_no_read;
DROP TABLE t_qrc_local_no_read;
SYSTEM DROP QUERY CACHE;
