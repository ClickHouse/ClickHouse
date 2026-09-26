-- Tags: no-parallel
-- Tag no-parallel: test_cluster_two_shards_different_databases reads from the shared databases shard_0 and shard_1

-- A `Distributed` table over a cross-replication cluster, whose shards resolve the remote table
-- in their own databases: an `additional_table_filters` entry for a table the query does not read
-- keeps parallel replicas allowed, and an entry naming the remote table in any database is refused.

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;
DROP TABLE IF EXISTS shard_0.t_05258;
DROP TABLE IF EXISTS shard_1.t_05258;
DROP TABLE IF EXISTS dist_05258;

CREATE TABLE shard_0.t_05258 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE shard_1.t_05258 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_0.t_05258 VALUES (0), (1);
INSERT INTO shard_1.t_05258 VALUES (0), (1);
CREATE TABLE dist_05258 (x UInt64) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 't_05258');

SELECT count() FROM dist_05258
SETTINGS additional_table_filters = {'system.one': 'dummy = 1'},
    enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0, parallel_replicas_plan_based = 0;

SELECT count() FROM dist_05258
SETTINGS additional_table_filters = {'shard_0.t_05258': 'x = 1'},
    enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0, parallel_replicas_plan_based = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE dist_05258;
DROP TABLE shard_0.t_05258;
DROP TABLE shard_1.t_05258;
