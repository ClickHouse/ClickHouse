DROP TABLE IF EXISTS 02918_parallel_replicas;

CREATE TABLE 02918_parallel_replicas (x String, y Int32) ENGINE = MergeTree ORDER BY cityHash64(x);

INSERT INTO 02918_parallel_replicas SELECT toString(number), number % 4 FROM numbers(1000);

SET prefer_localhost_replica=0;

-- { echoOn }
SELECT y, count()
FROM cluster(test_cluster_1_shard_3_replicas_1_unavailable, currentDatabase(), 02918_parallel_replicas)
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=3, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_custom_key_filter_type='default';

SELECT y, count()
FROM cluster(test_cluster_1_shard_3_replicas_1_unavailable, currentDatabase(), 02918_parallel_replicas)
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=3, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_custom_key_filter_type='range';

SET use_hedged_requests=0;
SELECT y, count()
FROM cluster(test_cluster_1_shard_3_replicas_1_unavailable, currentDatabase(), 02918_parallel_replicas)
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=3, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_custom_key_filter_type='default';
-- { echoOff }

DROP TABLE 02918_parallel_replicas;
