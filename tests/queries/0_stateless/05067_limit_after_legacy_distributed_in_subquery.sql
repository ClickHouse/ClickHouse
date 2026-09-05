-- Tags: distributed

-- The shards process an `ORDER BY` query with a range up to the stage after aggregation, and the
-- initiator applies the range over the merged stream. The boundary must therefore be analyzed for
-- execution on the initiator: here its `IN (subquery)` set has to be built.
SELECT shardNum() AS s, n FROM cluster(test_cluster_two_shards, system.one) ARRAY JOIN range(3) AS n ORDER BY s, n LIMIT 3 AFTER n IN (SELECT 1) SETTINGS enable_analyzer = 0;
SELECT shardNum() AS s, n FROM cluster(test_cluster_two_shards, system.one) ARRAY JOIN range(3) AS n ORDER BY s, n LIMIT UNTIL n IN (SELECT 1) SETTINGS enable_analyzer = 0;

SELECT shardNum() AS s, n FROM cluster(test_cluster_two_shards, system.one) ARRAY JOIN range(3) AS n ORDER BY s, n LIMIT 3 AFTER n IN (SELECT 1) SETTINGS enable_analyzer = 1;
SELECT shardNum() AS s, n FROM cluster(test_cluster_two_shards, system.one) ARRAY JOIN range(3) AS n ORDER BY s, n LIMIT UNTIL n IN (SELECT 1) SETTINGS enable_analyzer = 1;
