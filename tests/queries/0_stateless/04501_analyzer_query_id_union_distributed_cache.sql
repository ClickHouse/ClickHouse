-- Tags: replica, shard

-- Guard for the analyzer function-resolution cache: queryID() captures the current query id at
-- build time and is re-evaluated per shard in a distributed query. Two identical queryID() calls in
-- the outer (initiator) scope and an inner clusterAllReplicas scope share the same tree hash, so they
-- share the cached FunctionBase. The distributed branch must still yield its own per-shard ids and
-- must not be collapsed to the initiator's value: the distinct count is therefore greater than one
-- (the outer initiator id plus the per-shard ids of the distributed branch).

SELECT count(DISTINCT q) > 1
FROM
(
    SELECT queryID() AS q
    UNION ALL
    SELECT queryID() AS q
    FROM clusterAllReplicas('test_cluster_two_shards', system.one)
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;

-- The inner distributed branch alone must also report per-shard ids (more than one distinct value).
SELECT count(DISTINCT queryID()) > 1
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
