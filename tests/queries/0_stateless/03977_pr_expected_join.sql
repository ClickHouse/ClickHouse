DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS v0;

SET enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);
CREATE VIEW v0 AS (SELECT 1 c0 FROM t0);
SELECT 1 FROM t0 tx RIGHT JOIN t0 ON TRUE RIGHT JOIN v0 ON t0.c0 = v0.c0;

SELECT '---';
SELECT * FROM (SELECT 1 FROM remote('localhost:9000', currentDatabase(), 't0') AS tx JOIN t0 ty ON TRUE RIGHT JOIN t0 ON TRUE);

DROP TABLE v0;
DROP TABLE t0;
