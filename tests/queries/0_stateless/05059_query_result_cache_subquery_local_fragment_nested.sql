-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- The Planner-level query result cache read path is banned while a distributed plan is being built
-- (see `05054_query_result_cache_subquery_read_ban_settings_escape`), with one exception: the
-- in-process local fragment of a distributed query is never serialized, so reads are safe there.
-- The exception must cover the whole fragment, not only its root node: the local shard plan of a
-- `Distributed` table plans its `IN` subquery through `SelectQueryOptions::subquery`, which clears
-- `is_local_plan_for_distributed_query` and therefore used to suppress the cache read for every
-- subquery of the fragment. The sticky `inside_local_plan_for_distributed_query` keeps the fact
-- that the enclosing fragment stays in this process.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_local_nested;
DROP TABLE IF EXISTS t_qrc_local_nested_dist;
CREATE TABLE t_qrc_local_nested (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_local_nested SELECT number FROM numbers(10);
CREATE TABLE t_qrc_local_nested_dist (k UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_qrc_local_nested);

-- Subquery caching is a Planner feature: with the old analyzer no cache probe happens at all.
-- Pin the analyzer, because some CI jobs run the whole suite with `enable_analyzer = 0`.
SET enable_analyzer = 1;
SET enable_reads_from_query_cache = 1;
-- `make_distributed_plan` rejects an aggregation with a `max_rows_to_group_by` limit, which some CI
-- profiles set.
SET max_rows_to_group_by = 0;

-- Both shards of `test_cluster_two_shards_localhost` are local, so the shard plans are built in this
-- process by `createLocalPlan`. 0 + 1 + ... + 4 = 10, twice (two shards read the same table).
SELECT sum(k) FROM t_qrc_local_nested_dist WHERE k IN (SELECT k FROM t_qrc_local_nested WHERE k < 5 SETTINGS use_query_cache = 1)
    SETTINGS make_distributed_plan = 1, log_comment = '05059_local_fragment_nested';

SYSTEM FLUSH LOGS query_log;
-- The subquery of the local fragment must probe the cache (a hit or a miss, depending on what the
-- cache holds); before the fix no probe happened at all.
SELECT sum(ProfileEvents['QueryCacheHits'] + ProfileEvents['QueryCacheMisses']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND is_initial_query
    AND log_comment = '05059_local_fragment_nested';

DROP TABLE t_qrc_local_nested_dist;
DROP TABLE t_qrc_local_nested;
SYSTEM DROP QUERY CACHE;
