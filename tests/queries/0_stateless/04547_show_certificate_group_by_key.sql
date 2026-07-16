-- Tags: replica, shard, no-fasttest
-- Tag no-fasttest: `showCertificate` needs SSL support and a configured server certificate.

-- Guard: `showCertificate` returns server-local state (the client certificate on the
-- initiator, the shard's own server certificate on the shards), so the planner must never
-- remove `GROUP BY showCertificate()` as a constant key: the per-shard groups would collapse
-- into the initiator's value. The key must stay in the aggregation plan.
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM clusterAllReplicas('test_cluster_two_shards', system.one)
    GROUP BY showCertificate()
)
WHERE explain LIKE '%Keys: showCertificate()%'
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;

SELECT count()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
GROUP BY showCertificate()
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
