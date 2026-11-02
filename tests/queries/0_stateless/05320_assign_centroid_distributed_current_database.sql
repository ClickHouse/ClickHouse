-- `assignCentroid` resolves its dictionary name on the server that evaluates it. A shard or a parallel replica evaluates
-- it in a session whose current database is `default`, so an unqualified name must be bound to the database of the initiator.

DROP TABLE IF EXISTS distributed_vectors;
DROP TABLE IF EXISTS vectors;
DROP DICTIONARY IF EXISTS centroids_dict;
DROP TABLE IF EXISTS centroids;

CREATE TABLE centroids (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO centroids VALUES (0, [0.0, 0.0]), (7, [10.0, 10.0]);

CREATE DICTIONARY centroids_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'centroids')) LAYOUT(FLAT(MAX_ARRAY_SIZE 1000)) LIFETIME(0);

CREATE TABLE vectors (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO vectors VALUES (1, [1.0, 1.0]), (2, [9.0, 9.0]);

CREATE TABLE distributed_vectors AS vectors ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), vectors, rand());

-- Both shards are addresses of this server; force real connections, whose current database is `default`.
SET prefer_localhost_replica = 0;

SELECT id, assignCentroid(v, 'centroids_dict') FROM distributed_vectors ORDER BY ALL;
SELECT count() FROM distributed_vectors WHERE assignCentroid(v, 'centroids_dict') = 7;
SELECT assignCentroid(v, 'centroids_dict') AS c, count() FROM distributed_vectors GROUP BY c ORDER BY c;
SELECT id, arrayMap(x -> assignCentroid(x, 'centroids_dict'), [v]) FROM distributed_vectors ORDER BY ALL;
SELECT id, assignCentroid(v, toNullable('centroids_dict')) AS c, toTypeName(c) FROM distributed_vectors ORDER BY ALL;
SELECT id, assignCentroid(v, 'centroids_dict') FROM cluster(test_cluster_two_shards, currentDatabase(), vectors) ORDER BY ALL;

-- Parallel replicas without the local plan: every replica is a connection whose current database is `default`.
SELECT id, assignCentroid(v, 'centroids_dict') FROM vectors ORDER BY ALL
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;

-- Binding the name does not change the name of the column.
SELECT assignCentroid(v, 'centroids_dict') FROM distributed_vectors ORDER BY ALL LIMIT 1 FORMAT TSVWithNames;

DROP TABLE distributed_vectors;
DROP TABLE vectors;
DROP DICTIONARY centroids_dict;
DROP TABLE centroids;
