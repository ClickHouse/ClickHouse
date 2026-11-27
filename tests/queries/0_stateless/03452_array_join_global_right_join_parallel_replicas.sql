-- Tags: shard

-- Reproducer for exception in buildQueryTreeForShard when ARRAY JOIN is combined with GLOBAL RIGHT JOIN
-- and parallel replicas are enabled.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a1 UInt64, a2 String, arr Array(UInt64)) ENGINE = MergeTree ORDER BY a1;
CREATE TABLE t2 (a2 String) ENGINE = MergeTree ORDER BY a2;

INSERT INTO t1 VALUES (1, 'x', [10, 20, 30]);
INSERT INTO t1 VALUES (2, 'y', [40, 50]);
INSERT INTO t2 VALUES ('x');
INSERT INTO t2 VALUES ('z');

SELECT a1, a2, arr
FROM t1
ARRAY JOIN arr
GLOBAL RIGHT JOIN t2 AS t ON t.a2 = t1.a2
ORDER BY a1, a2, arr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT a1, a2, arr
FROM t1
ARRAY JOIN arr
GLOBAL LEFT JOIN t2 AS t ON t.a2 = t1.a2
ORDER BY a1, a2, arr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE t1;
DROP TABLE t2;
