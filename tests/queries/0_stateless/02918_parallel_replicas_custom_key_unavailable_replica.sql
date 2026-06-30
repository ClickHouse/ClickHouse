DROP TABLE IF EXISTS 02918_parallel_replicas;

CREATE TABLE 02918_parallel_replicas (x String, y Int32) ENGINE = MergeTree ORDER BY cityHash64(x);

INSERT INTO 02918_parallel_replicas SELECT toString(number), number % 4 FROM numbers(1000);

SET prefer_localhost_replica=0;

--- if we try to query unavaialble replica, connection will be retried
--- but a warning log message will be printed out
SET send_logs_level='error';
-- { echoOn }
SELECT y, count()
FROM cluster(test_cluster_1_shard_3_replicas_1_unavailable, currentDatabase(), 02918_parallel_replicas)
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=3, enable_parallel_replicas=1, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_mode='custom_key_sampling';

SELECT y, count()
FROM cluster(test_cluster_1_shard_3_replicas_1_unavailable, currentDatabase(), 02918_parallel_replicas)
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=3, enable_parallel_replicas=1, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_mode='custom_key_range';

SET use_hedged_requests=0;
SELECT y, count()
FROM cluster(test_cluster_1_shard_3_replicas_1_unavailable, currentDatabase(), 02918_parallel_replicas)
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=3, enable_parallel_replicas=1, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_mode='custom_key_sampling';
-- { echoOff }
SET send_logs_level='warning';

DROP TABLE 02918_parallel_replicas;
