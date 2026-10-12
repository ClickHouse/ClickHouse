DROP TABLE IF EXISTS 02898_parallel_replicas_final;

CREATE TABLE 02898_parallel_replicas_final (x String, y Int32) ENGINE = ReplacingMergeTree ORDER BY cityHash64(x);

INSERT INTO 02898_parallel_replicas_final SELECT toString(number), number % 3 FROM numbers(1000);

SELECT y, count()
FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 02898_parallel_replicas_final) FINAL
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=3, enable_parallel_replicas=1, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_mode='custom_key_sampling';

SELECT y, count()
FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 02898_parallel_replicas_final) FINAL
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=3, enable_parallel_replicas=1, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_mode='custom_key_range';

DROP TABLE 02898_parallel_replicas_final;
