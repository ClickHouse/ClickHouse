-- Tags: distributed

-- Regression: shard-local execution (via `optimize_distributed_group_by_sharding_key`)
-- used to silently apply `WITH CLUSTER` only within each shard's data,
-- missing pairs that cross shard boundaries. Force `WithMergeableState`
-- for `WITH CLUSTER` queries so the cluster merge runs once on the
-- initiator against the union of all shards' aggregate states.

SET enable_analyzer = 1;
SET optimize_skip_unused_shards = 1;
SET optimize_distributed_group_by_sharding_key = 1;
SET prefer_localhost_replica = 1;

-- Each shard generates two rows offset by `shardNum()`. Across both shards
-- the distinct values are 0, 1, 2, 3 — adjacent pairs are within
-- `WITH CLUSTER 1`, so chain semantics merges them into one cluster.
-- Without the fix the shards would cluster locally ({0, 2} and {1, 3},
-- distance 2 each, no merge) and the initiator would just concatenate
-- four separate clusters.
SELECT count() AS num_clusters
FROM (
    SELECT x, count() AS c
    FROM remote('127.0.0.{1,2}', view(
        SELECT (number * 2 + shardNum() - 1)::UInt64 AS x FROM numbers(2)
    ), x)
    GROUP BY x WITH CLUSTER 1
);

-- Same query under `distributed_group_by_no_merge = 2`, which would normally
-- force `WithMergeableStateAfterAggregation` and skip the initiator-side
-- cluster merging. The stage cap in `StorageDistributed::getQueryProcessingStage`
-- must still hold the shard stage at `WithMergeableState`.
SET distributed_group_by_no_merge = 2;
SELECT count() AS num_clusters
FROM (
    SELECT x, count() AS c
    FROM remote('127.0.0.{1,2}', view(
        SELECT (number * 2 + shardNum() - 1)::UInt64 AS x FROM numbers(2)
    ), x)
    GROUP BY x WITH CLUSTER 1
);
SET distributed_group_by_no_merge = 0;
