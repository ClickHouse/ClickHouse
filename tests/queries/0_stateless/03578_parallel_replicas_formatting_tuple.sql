SET enable_analyzer = 1,
    max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    enable_parallel_replicas = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (s String) ORDER BY ();
INSERT INTO test VALUES ('a'), ('b');
SELECT transform(s, ['a', 'b'], [(1, 2), (3, 4)], (0, 0)) FROM test ORDER BY ALL;
SELECT s != '' ? (1,2) : (0,0) FROM test;

DROP TABLE test;
