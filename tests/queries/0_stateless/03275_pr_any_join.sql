DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;

CREATE TABLE t1 (x UInt32, s String) engine = ReplicatedMergeTree('/clickhouse/{database}/t1', 'r1') ORDER BY tuple();
CREATE TABLE t2 (x UInt32, s String) engine = ReplicatedMergeTree('/clickhouse/{database}/t2', 'r1') ORDER BY tuple();

INSERT INTO t1 (x, s) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5');
INSERT INTO t2 (x, s) VALUES (2, 'b1'), (4, 'b2'), (5, 'b4');

set enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT 'any left';
SELECT t1.*, t2.* FROM t1 ANY LEFT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any left (rev)';
SELECT t1.*, t2.* FROM t2 ANY LEFT JOIN t1 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any inner';
SELECT t1.*, t2.* FROM t1 ANY INNER JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any inner (rev)';
SELECT t1.*, t2.* FROM t2 ANY INNER JOIN t1 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any right';
SELECT t1.*, t2.* FROM t1 ANY RIGHT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'any right (rev)';
SELECT t1.*, t2.* FROM t2 ANY RIGHT JOIN t1 USING(x) ORDER BY t1.x, t2.x;

DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
