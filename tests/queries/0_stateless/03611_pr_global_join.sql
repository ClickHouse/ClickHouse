DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (c0 Int NULL, c1 Int) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t2 (c0 Int NULL, c1 Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE t1 (c1, c0) VALUES (1, 1);
INSERT INTO TABLE t2 (c0, c1) VALUES (1, 1);

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT *
FROM t2
INNER JOIN t2 AS tx ON true
RIGHT JOIN t1 ON true;

SELECT '---';

-- this query was problematic, now GLOBAL JOINs has been disabled in n-way JOINs
SELECT *
FROM t2
INNER JOIN t2 AS tx ON true
GLOBAL RIGHT JOIN t1 ON true;

SELECT '---';
-- just check that simple GLOBAL JOIN works with parallel replicas
SELECT *
FROM t2 GLOBAL RIGHT JOIN t1 ON true;

DROP TABLE t1;
DROP TABLE t2;
