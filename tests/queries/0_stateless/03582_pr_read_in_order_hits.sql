-- Tags: stateful
SET max_threads = 0; -- let's reset to automatic detection of the number of threads, otherwise test can be slow.

SELECT '--- In order ---';
SELECT 'Result hash   : ', cityHash64(groupArray(CounterID))
FROM
(
    SELECT CounterID
    FROM test.hits
    WHERE domain(URL) IN ('yandex.ru', 'auto.ru', 'avito.ru')
    ORDER BY CounterID
    SETTINGS enable_parallel_replicas = 0
);

SELECT 'PR result hash: ', cityHash64(groupArray(CounterID))
FROM
(
    SELECT CounterID
    FROM test.hits
    WHERE domain(URL) IN ('yandex.ru', 'auto.ru', 'avito.ru')
    ORDER BY CounterID
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'
);
SELECT '-- In reverse order ---';
SELECT 'Result hash   : ', cityHash64(groupArray(CounterID))
FROM
(
    SELECT CounterID
    FROM test.hits
    WHERE domain(URL) IN ('yandex.ru', 'auto.ru', 'avito.ru')
    ORDER BY CounterID DESC
    SETTINGS enable_parallel_replicas = 0
);

SELECT 'PR result hash: ', cityHash64(groupArray(CounterID))
FROM
(
    SELECT CounterID
    FROM test.hits
    WHERE domain(URL) IN ('yandex.ru', 'auto.ru', 'avito.ru')
    ORDER BY CounterID DESC
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'
);
