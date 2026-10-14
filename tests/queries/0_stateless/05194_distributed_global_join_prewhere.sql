-- Tags: distributed

SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS left_local_05194;
DROP TABLE IF EXISTS right_local_05194;
DROP TABLE IF EXISTS left_distributed_05194;
DROP TABLE IF EXISTS right_distributed_05194;
DROP TABLE IF EXISTS left_one_shard_05194;

CREATE TABLE left_local_05194 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE right_local_05194 (k2 UInt32, v2 String)
ENGINE = MergeTree
ORDER BY k2;

CREATE TABLE left_distributed_05194 AS left_local_05194
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), left_local_05194);

CREATE TABLE right_distributed_05194 AS right_local_05194
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), right_local_05194);

CREATE TABLE left_one_shard_05194 AS left_local_05194
ENGINE = Distributed('test_shard_localhost', currentDatabase(), left_local_05194);

INSERT INTO left_local_05194 VALUES (1, 'a'), (2, 'b');
INSERT INTO right_local_05194 VALUES (1, 'A'), (2, 'B'), (3, 'C');

SELECT 'without_prewhere';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_distributed_05194 AS l
GLOBAL RIGHT JOIN right_distributed_05194 AS r ON l.k1 = r.k2
GROUP BY ALL
ORDER BY ALL;

SELECT 'issue_prewhere';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_distributed_05194 AS l
GLOBAL RIGHT JOIN right_distributed_05194 AS r ON l.k1 = r.k2
PREWHERE l.v1 != ''
GROUP BY ALL
ORDER BY ALL;

SELECT 'left_prewhere';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_distributed_05194 AS l
GLOBAL RIGHT JOIN right_distributed_05194 AS r ON l.k1 = r.k2
PREWHERE l.v1 = 'a'
GROUP BY ALL
ORDER BY ALL;

SELECT 'right_prewhere';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_distributed_05194 AS l
GLOBAL RIGHT JOIN right_distributed_05194 AS r ON l.k1 = r.k2
PREWHERE r.v2 != 'B'
GROUP BY ALL
ORDER BY ALL;

SELECT 'constant_prewhere';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_distributed_05194 AS l
GLOBAL RIGHT JOIN right_distributed_05194 AS r ON l.k1 = r.k2
PREWHERE 0
GROUP BY ALL
ORDER BY ALL;

SELECT 'distributed_product_mode';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_distributed_05194 AS l
RIGHT JOIN right_distributed_05194 AS r ON l.k1 = r.k2
PREWHERE l.v1 = 'a'
GROUP BY ALL
ORDER BY ALL
SETTINGS distributed_product_mode = 'global';

SELECT 'prewhere_with_set';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_distributed_05194 AS l
GLOBAL RIGHT JOIN right_distributed_05194 AS r ON l.k1 = r.k2
PREWHERE l.k1 IN (SELECT 1)
GROUP BY ALL
ORDER BY ALL;

SELECT 'full_join';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_distributed_05194 AS l
GLOBAL FULL JOIN right_distributed_05194 AS r ON l.k1 = r.k2
PREWHERE l.v1 = 'a'
GROUP BY ALL
ORDER BY ALL;

SELECT 'one_shard';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_one_shard_05194 AS l
GLOBAL RIGHT JOIN right_distributed_05194 AS r ON l.k1 = r.k2
PREWHERE l.v1 = 'a'
GROUP BY ALL
ORDER BY ALL;

SELECT 'remote_right_prewhere';
SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_local_05194 AS l
INNER JOIN right_distributed_05194 AS r ON l.k1 = r.k2
PREWHERE r.v2 = 'A'
GROUP BY ALL
ORDER BY ALL;

DROP TABLE left_one_shard_05194;
DROP TABLE left_distributed_05194;
DROP TABLE right_distributed_05194;
DROP TABLE left_local_05194;
DROP TABLE right_local_05194;
