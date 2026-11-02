-- Tags: distributed

SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS left_local_05140;
DROP TABLE IF EXISTS right_local_05140;
DROP TABLE IF EXISTS left_distributed_05140;
DROP TABLE IF EXISTS right_distributed_05140;
DROP TABLE IF EXISTS third_local_05140;
DROP TABLE IF EXISTS third_distributed_05140;

CREATE TABLE left_local_05140 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE right_local_05140 (k2 UInt32, v2 String)
ENGINE = MergeTree
ORDER BY k2;

CREATE TABLE left_distributed_05140 AS left_local_05140
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), left_local_05140);

CREATE TABLE right_distributed_05140 AS right_local_05140
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), right_local_05140);

INSERT INTO left_local_05140 VALUES (1, 'a'), (2, 'b'), (4, 'd');
INSERT INTO right_local_05140 VALUES (1, 'A'), (2, 'B'), (3, 'C');

-- A RIGHT join anywhere in the tree emits the preserved rows once per shard, not only at the root.
-- The third table is a superset of the keys, otherwise the outer join filters out exactly the
-- duplicated rows and the test passes while the bug is still there.
CREATE TABLE third_local_05140 (k3 UInt32, v3 String)
ENGINE = MergeTree
ORDER BY k3;

CREATE TABLE third_distributed_05140 AS third_local_05140
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), third_local_05140);

INSERT INTO third_local_05140 VALUES (1, 'X'), (2, 'Y'), (3, 'Z'), (4, 'W');

SELECT 'nested_right_then_inner_initiator';
SELECT k1, v1, k2, v2, k3, v3
FROM (SELECT * FROM left_distributed_05140) AS l
RIGHT JOIN right_distributed_05140 AS r ON l.k1 = r.k2
INNER JOIN third_distributed_05140 AS t ON r.k2 = t.k3
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'nested_right_then_inner_global';
SELECT k1, v1, k2, v2, k3, v3
FROM left_distributed_05140 AS l
GLOBAL RIGHT JOIN right_distributed_05140 AS r ON l.k1 = r.k2
GLOBAL INNER JOIN third_distributed_05140 AS t ON r.k2 = t.k3
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'nested_right_then_right_initiator';
SELECT k1, v1, k2, v2, k3, v3
FROM (SELECT * FROM left_distributed_05140) AS l
RIGHT JOIN right_distributed_05140 AS r ON l.k1 = r.k2
RIGHT JOIN third_distributed_05140 AS t ON r.k2 = t.k3
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'nested_right_then_right_global';
SELECT k1, v1, k2, v2, k3, v3
FROM left_distributed_05140 AS l
GLOBAL RIGHT JOIN right_distributed_05140 AS r ON l.k1 = r.k2
GLOBAL RIGHT JOIN third_distributed_05140 AS t ON r.k2 = t.k3
ORDER BY ALL
FORMAT TSVWithNames;

-- Here the RIGHT join sits at the root and its left side is another join, so the sides cannot be
-- swapped. The join runs on the initiator instead.
SELECT 'nested_inner_then_right_initiator';
SELECT k1, v1, k2, v2, k3, v3
FROM (SELECT * FROM left_distributed_05140) AS l
INNER JOIN third_distributed_05140 AS t ON l.k1 = t.k3
RIGHT JOIN right_distributed_05140 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'nested_inner_then_right_global';
SELECT k1, v1, k2, v2, k3, v3
FROM left_distributed_05140 AS l
GLOBAL INNER JOIN third_distributed_05140 AS t ON l.k1 = t.k3
GLOBAL RIGHT JOIN right_distributed_05140 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

-- FULL JOIN preserves the broadcast side too, and cannot be repaired by swapping.
SELECT 'full_join_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_distributed_05140) AS l
FULL JOIN right_distributed_05140 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'full_join_global';
SELECT k1, v1, k2, v2
FROM left_distributed_05140 AS l
GLOBAL FULL JOIN right_distributed_05140 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

-- Trees without a RIGHT or FULL join keep running on the shards, so shardNum still varies there.
SELECT 'shard_num_is_untouched_without_right_join';
SELECT DISTINCT shardNum() AS s
FROM left_distributed_05140 AS l
GLOBAL INNER JOIN right_distributed_05140 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

DROP TABLE third_distributed_05140;
DROP TABLE third_local_05140;
DROP TABLE left_distributed_05140;
DROP TABLE right_distributed_05140;
DROP TABLE left_local_05140;
DROP TABLE right_local_05140;
