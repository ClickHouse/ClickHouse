-- Tags: distributed

SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS left_local_05138;
DROP TABLE IF EXISTS right_local_05138;
DROP TABLE IF EXISTS left_distributed_05138;
DROP TABLE IF EXISTS right_distributed_05138;
DROP TABLE IF EXISTS shared_left_local_05138;
DROP TABLE IF EXISTS shared_right_local_05138;
DROP TABLE IF EXISTS shared_left_distributed_05138;
DROP TABLE IF EXISTS shared_right_distributed_05138;

CREATE TABLE left_local_05138 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE right_local_05138 (k2 UInt32, v2 String)
ENGINE = MergeTree
ORDER BY k2;

CREATE TABLE left_distributed_05138 AS left_local_05138
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), left_local_05138);

CREATE TABLE right_distributed_05138 AS right_local_05138
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), right_local_05138);

INSERT INTO left_local_05138 VALUES (1, 'a'), (2, 'b'), (4, 'd');
INSERT INTO right_local_05138 VALUES (1, 'A'), (2, 'B'), (3, 'C');

CREATE TABLE shared_left_local_05138 (k UInt32, k2 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k;

CREATE TABLE shared_right_local_05138 (k UInt32, k2 Int64, v2 String)
ENGINE = MergeTree
ORDER BY k;

CREATE TABLE shared_left_distributed_05138 AS shared_left_local_05138
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), shared_left_local_05138);

CREATE TABLE shared_right_distributed_05138 AS shared_right_local_05138
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), shared_right_local_05138);

INSERT INTO shared_left_local_05138 VALUES (1, 1, 'a'), (2, 2, 'b'), (4, 4, 'd');
INSERT INTO shared_right_local_05138 VALUES (1, 1, 'A'), (2, 2, 'B'), (3, 3, 'C');

SELECT 'using_initiator';
SELECT k, v1, v2
FROM (SELECT * FROM shared_left_distributed_05138) AS l
RIGHT JOIN (SELECT * FROM shared_right_distributed_05138) AS r USING (k)
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'using_global';
SELECT k, v1, v2
FROM shared_left_distributed_05138 AS l
GLOBAL RIGHT JOIN shared_right_distributed_05138 AS r USING (k)
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'using_distributed_product_mode';
SELECT k, v1, v2
FROM shared_left_distributed_05138 AS l
RIGHT JOIN shared_right_distributed_05138 AS r USING (k)
ORDER BY ALL
SETTINGS distributed_product_mode = 'global'
FORMAT TSVWithNames;

-- Two keys at once, the second one needing a common supertype.
SELECT 'using_multiple_keys_global';
SELECT k, k2, v1, v2
FROM shared_left_distributed_05138 AS l
GLOBAL RIGHT JOIN shared_right_distributed_05138 AS r USING (k, k2)
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'natural_initiator';
SELECT *
FROM (SELECT k, v1 FROM shared_left_distributed_05138) AS l
NATURAL RIGHT JOIN (SELECT k, v2 FROM shared_right_distributed_05138) AS r
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'natural_global';
SELECT *
FROM (SELECT k, v1 FROM shared_left_distributed_05138) AS l
GLOBAL NATURAL RIGHT JOIN (SELECT k, v2 FROM shared_right_distributed_05138) AS r
ORDER BY ALL
FORMAT TSVWithNames;

-- USING (a AS b) takes the key from the left table as `a` and from the right one as `b`.
SELECT 'using_alias_initiator';
SELECT *
FROM (SELECT * FROM left_distributed_05138) AS l
RIGHT JOIN (SELECT * FROM right_distributed_05138) AS r USING (k1 AS k2)
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'using_alias_global';
SELECT *
FROM left_distributed_05138 AS l
GLOBAL RIGHT JOIN right_distributed_05138 AS r USING (k1 AS k2)
ORDER BY ALL
FORMAT TSVWithNames;

-- LEFT JOIN is not rewritten and must keep working.
SELECT 'using_left_join_global';
SELECT k, v1, v2
FROM shared_left_distributed_05138 AS l
GLOBAL LEFT JOIN shared_right_distributed_05138 AS r USING (k)
ORDER BY ALL
FORMAT TSVWithNames;

DROP TABLE shared_left_distributed_05138;
DROP TABLE shared_right_distributed_05138;
DROP TABLE shared_left_local_05138;
DROP TABLE shared_right_local_05138;
DROP TABLE left_distributed_05138;
DROP TABLE right_distributed_05138;
DROP TABLE left_local_05138;
DROP TABLE right_local_05138;
