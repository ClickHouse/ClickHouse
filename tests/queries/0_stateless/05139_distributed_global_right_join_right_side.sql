-- Tags: distributed

SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS left_local_05139;
DROP TABLE IF EXISTS right_local_05139;
DROP TABLE IF EXISTS left_distributed_05139;
DROP TABLE IF EXISTS right_distributed_05139;
DROP TABLE IF EXISTS right_one_shard_05139;
DROP TABLE IF EXISTS left_one_shard_05139;
DROP TABLE IF EXISTS dup_left_local_05139;
DROP TABLE IF EXISTS dup_left_distributed_05139;

CREATE TABLE left_local_05139 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE right_local_05139 (k2 UInt32, v2 String)
ENGINE = MergeTree
ORDER BY k2;

CREATE TABLE left_distributed_05139 AS left_local_05139
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), left_local_05139);

CREATE TABLE right_distributed_05139 AS right_local_05139
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), right_local_05139);

INSERT INTO left_local_05139 VALUES (1, 'a'), (2, 'b'), (4, 'd');
INSERT INTO right_local_05139 VALUES (1, 'A'), (2, 'B'), (3, 'C');

CREATE TABLE right_one_shard_05139 AS right_local_05139
ENGINE = Distributed('test_shard_localhost', currentDatabase(), right_local_05139);

SELECT 'right_is_local_table_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_distributed_05139) AS l
RIGHT JOIN right_local_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_local_table_global';
SELECT k1, v1, k2, v2
FROM left_distributed_05139 AS l
GLOBAL RIGHT JOIN right_local_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_one_shard_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_distributed_05139) AS l
RIGHT JOIN right_one_shard_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_one_shard_global';
SELECT k1, v1, k2, v2
FROM left_distributed_05139 AS l
GLOBAL RIGHT JOIN right_one_shard_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_subquery_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_distributed_05139) AS l
RIGHT JOIN (SELECT * FROM right_distributed_05139) AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_subquery_global';
SELECT k1, v1, k2, v2
FROM left_distributed_05139 AS l
GLOBAL RIGHT JOIN (SELECT * FROM right_distributed_05139) AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_table_function_initiator';
SELECT k1, v1, n
FROM (SELECT * FROM left_distributed_05139) AS l
RIGHT JOIN (SELECT number AS n FROM numbers(5)) AS r ON l.k1 = r.n
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_table_function_global';
SELECT k1, v1, n
FROM left_distributed_05139 AS l
GLOBAL RIGHT JOIN (SELECT number AS n FROM numbers(5)) AS r ON l.k1 = r.n
ORDER BY ALL
FORMAT TSVWithNames;

-- ANY, SEMI and ANTI mirror into their LEFT counterparts, so they are rewritten as well.
-- The duplicate key on the left makes ANY and ALL return a different number of rows.
CREATE TABLE dup_left_local_05139 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE dup_left_distributed_05139 AS dup_left_local_05139
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), dup_left_local_05139);

INSERT INTO dup_left_local_05139 VALUES (1, 'a1'), (1, 'a2'), (2, 'b'), (4, 'd');

SELECT 'any_initiator';
SELECT k2, v2
FROM (SELECT * FROM dup_left_distributed_05139) AS l
RIGHT ANY JOIN right_local_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'any_global';
SELECT k2, v2
FROM dup_left_distributed_05139 AS l
GLOBAL RIGHT ANY JOIN right_local_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'semi_initiator';
SELECT k2, v2
FROM (SELECT * FROM dup_left_distributed_05139) AS l
RIGHT SEMI JOIN right_local_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'semi_global';
SELECT k2, v2
FROM dup_left_distributed_05139 AS l
GLOBAL RIGHT SEMI JOIN right_local_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'anti_initiator';
SELECT k2, v2
FROM (SELECT * FROM dup_left_distributed_05139) AS l
RIGHT ANTI JOIN right_local_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'anti_global';
SELECT k2, v2
FROM dup_left_distributed_05139 AS l
GLOBAL RIGHT ANTI JOIN right_local_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

-- A single shard on the left never duplicates anything, so nothing is rewritten there.
CREATE TABLE left_one_shard_05139 AS left_local_05139
ENGINE = Distributed('test_shard_localhost', currentDatabase(), left_local_05139);

SELECT 'left_is_one_shard_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_one_shard_05139) AS l
RIGHT JOIN right_distributed_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'left_is_one_shard_global';
SELECT k1, v1, k2, v2
FROM left_one_shard_05139 AS l
GLOBAL RIGHT JOIN right_distributed_05139 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

DROP TABLE left_one_shard_05139;
DROP TABLE dup_left_distributed_05139;
DROP TABLE dup_left_local_05139;
DROP TABLE right_one_shard_05139;
DROP TABLE left_distributed_05139;
DROP TABLE right_distributed_05139;
DROP TABLE left_local_05139;
DROP TABLE right_local_05139;
