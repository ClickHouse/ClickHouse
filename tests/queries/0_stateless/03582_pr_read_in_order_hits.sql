-- Tags: stateful
SET max_threads = 0; -- let's reset to automatic detection of the number of threads, otherwise test can be slow.

SELECT DISTINCT
    CounterID,
FROM test.hits
WHERE (URL ILIKE '%rambler%') AND (EventDate = '2013-07-15')
ORDER BY CounterID DESC
SETTINGS enable_parallel_replicas = 0;

SELECT '-----------';

SELECT DISTINCT
    CounterID,
FROM test.hits
WHERE (URL ILIKE '%rambler%') AND (EventDate = '2013-07-15')
ORDER BY CounterID DESC
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
