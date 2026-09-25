-- Tags: no-parallel, no-fasttest
-- - no-parallel - the query result cache is shared
-- - no-fasttest - the `TimeSeries` table engine is experimental and disabled in the fast-test build

-- The Planner-level query result cache read path is banned while a distributed plan is being built,
-- except inside the in-process local fragment of a distributed query (see
-- `05059_query_result_cache_subquery_local_fragment_nested`). A `TimeSeries` table plans a generated
-- query of its own for the read (`StorageTimeSeries::readImpl`), and it used to start from fresh
-- `SelectQueryOptions`, so the subqueries of the generated query lost the exemption inside a local
-- fragment, like the body of a `View` did (`05218_query_result_cache_subquery_local_fragment_view`).
-- The fact is now carried through `SelectQueryInfo`.

SYSTEM DROP QUERY CACHE;

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS t_qrc_local_ts_dist;
DROP TABLE IF EXISTS t_qrc_local_ts;
-- recent_samples_ttl_seconds = 0 disables the recent samples table, so the generated query reads the
-- samples inner table through its `GROUP BY id` subquery.
CREATE TABLE t_qrc_local_ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
INSERT INTO t_qrc_local_ts (metric_name, tags, samples) VALUES
    ('m', map('n', 'a'), [(toDateTime64('2024-01-01 00:00:00', 3, 'UTC'), 1.), (toDateTime64('2024-01-01 00:00:15', 3, 'UTC'), 2.)]),
    ('m', map('n', 'b'), [(toDateTime64('2024-01-01 00:00:00', 3, 'UTC'), 10.)]);
CREATE TABLE t_qrc_local_ts_dist (metric_name LowCardinality(String), samples Array(Tuple(DateTime64(3, 'UTC'), Float64)))
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_qrc_local_ts);

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
-- process by `createLocalPlan`, and each of them reads the `TimeSeries` table. The generated query has
-- no explicit `use_query_cache`, so the subqueries are made cacheable by `query_cache_for_subqueries`,
-- which needs `use_query_cache` on the outer query.
SELECT count(), sum(length(samples)) FROM t_qrc_local_ts_dist
    SETTINGS make_distributed_plan = 1, use_query_cache = 1, query_cache_for_subqueries = 1,
        log_comment = '05255_local_fragment_time_series';

SYSTEM FLUSH LOGS query_log;
-- The outer query probes the cache once in `executeQuery`; the subqueries of the generated query must
-- probe it as well (a hit or a miss, depending on what the cache holds). Before the fix only the
-- outer query probed.
SELECT sum(ProfileEvents['QueryCacheHits'] + ProfileEvents['QueryCacheMisses']) > 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND is_initial_query
    AND log_comment = '05255_local_fragment_time_series';

DROP TABLE t_qrc_local_ts_dist;
DROP TABLE t_qrc_local_ts;
SYSTEM DROP QUERY CACHE;
