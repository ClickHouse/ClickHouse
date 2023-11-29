DROP TABLE IF EXISTS 02918_parallel_replicas;

CREATE TABLE 02918_parallel_replicas (x String, y Int32) ENGINE = MergeTree ORDER BY cityHash64(x);

INSERT INTO 02918_parallel_replicas SELECT toString(number), number % 4 FROM numbers(1000);

-- SET async_socket_for_remote=0;
-- SET async_query_sending_for_remote=0;
-- SET load_balancing='round_robin';
-- SET use_hedged_requests=0;

-- { echoOn }
SELECT y, count()
FROM cluster(parallel_replicas_custom_key_unavailable_replica, currentDatabase(), 02918_parallel_replicas)
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=2, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_custom_key_filter_type='default';

SELECT y, count()
FROM cluster(parallel_replicas_custom_key_unavailable_replica, currentDatabase(), 02918_parallel_replicas)
GROUP BY y
ORDER BY y
SETTINGS max_parallel_replicas=3, parallel_replicas_custom_key='cityHash64(y)', parallel_replicas_custom_key_filter_type='range';

-- { echoOff }

DROP TABLE 02918_parallel_replicas;
