-- Tags: shard

-- Asking whether shipping a query would materialize its subqueries must not apply
-- `distributed_product_mode`. That setting is a policy the shipping path enforces by throwing, and
-- its default is `deny`, which rejects any `Distributed` table of two or more shards - so a question
-- asked on behalf of a query that is not being shipped would fail the query outright. The
-- automatic-parallel-replicas decision asks this for every query it considers.

DROP TABLE IF EXISTS data_05234;
DROP TABLE IF EXISTS dist_05234;

CREATE TABLE data_05234 (key Int, value String) ENGINE = MergeTree ORDER BY key;
-- Big enough that the decision actually considers the query instead of declining on size.
INSERT INTO data_05234 SELECT number, toString(number) FROM numbers(1000000);

CREATE TABLE dist_05234 AS data_05234
    ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data_05234, key % 2);

SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 1, automatic_parallel_replicas_min_bytes_per_replica = 0;

-- The default, spelled out: the query below reads a `Distributed` table and nothing else, so nothing
-- about it is denied and nothing about it is materialized.
SET distributed_product_mode = 'deny';

-- Shard pruning is what takes the query down the path that used to ask the question, and it is
-- randomized, so pin it. It also makes the result deterministic: both shards of
-- `test_cluster_two_shards` are this same server, so without pruning the row comes back twice.
SET optimize_skip_unused_shards = 1;

SELECT key, value FROM dist_05234 WHERE key = 0 ORDER BY key;

DROP TABLE dist_05234;
DROP TABLE data_05234;
