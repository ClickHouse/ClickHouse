-- Tags: distributed

SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS left_local_05137;
DROP TABLE IF EXISTS right_local_05137;
DROP TABLE IF EXISTS left_distributed_05137;
DROP TABLE IF EXISTS right_distributed_05137;

CREATE TABLE left_local_05137 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE right_local_05137 (k2 UInt32, v2 String)
ENGINE = MergeTree
ORDER BY k2;

CREATE TABLE left_distributed_05137 AS left_local_05137
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), left_local_05137);

CREATE TABLE right_distributed_05137 AS right_local_05137
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), right_local_05137);

INSERT INTO left_local_05137 VALUES (1, 'a'), (2, 'b'), (4, 'd');
INSERT INTO right_local_05137 VALUES (1, 'A'), (2, 'B'), (3, 'C');

-- The subqueries force the join to run on the initiator and define the correct result.
SELECT 'initiator';
SELECT *
FROM (SELECT * FROM left_distributed_05137) AS l
RIGHT JOIN (SELECT * FROM right_distributed_05137) AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

-- The global join must produce the same rows and preserve the original column order.
SELECT 'distributed_product_mode';
SELECT *
FROM left_distributed_05137 AS l
RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
SETTINGS distributed_product_mode = 'global'
FORMAT TSVWithNames;

SELECT 'explicit_global';
SELECT *
FROM left_distributed_05137 AS l
GLOBAL RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

-- The same rewrite must happen for the USING carrier, and for NATURAL, which the analyzer turns into USING.
DROP TABLE IF EXISTS shared_left_local_05137;
DROP TABLE IF EXISTS shared_right_local_05137;
DROP TABLE IF EXISTS shared_left_distributed_05137;
DROP TABLE IF EXISTS shared_right_distributed_05137;

CREATE TABLE shared_left_local_05137 (k UInt32, k2 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k;

CREATE TABLE shared_right_local_05137 (k UInt32, k2 Int64, v2 String)
ENGINE = MergeTree
ORDER BY k;

CREATE TABLE shared_left_distributed_05137 AS shared_left_local_05137
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), shared_left_local_05137);

CREATE TABLE shared_right_distributed_05137 AS shared_right_local_05137
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), shared_right_local_05137);

INSERT INTO shared_left_local_05137 VALUES (1, 1, 'a'), (2, 2, 'b'), (4, 4, 'd');
INSERT INTO shared_right_local_05137 VALUES (1, 1, 'A'), (2, 2, 'B'), (3, 3, 'C');

SELECT 'using_initiator';
SELECT k, v1, v2
FROM (SELECT * FROM shared_left_distributed_05137) AS l
RIGHT JOIN (SELECT * FROM shared_right_distributed_05137) AS r USING (k)
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'using_global';
SELECT k, v1, v2
FROM shared_left_distributed_05137 AS l
GLOBAL RIGHT JOIN shared_right_distributed_05137 AS r USING (k)
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'using_distributed_product_mode';
SELECT k, v1, v2
FROM shared_left_distributed_05137 AS l
RIGHT JOIN shared_right_distributed_05137 AS r USING (k)
ORDER BY ALL
SETTINGS distributed_product_mode = 'global'
FORMAT TSVWithNames;

-- Two keys at once, the second one needing a common supertype.
SELECT 'using_multiple_keys_global';
SELECT k, k2, v1, v2
FROM shared_left_distributed_05137 AS l
GLOBAL RIGHT JOIN shared_right_distributed_05137 AS r USING (k, k2)
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'natural_initiator';
SELECT *
FROM (SELECT k, v1 FROM shared_left_distributed_05137) AS l
NATURAL RIGHT JOIN (SELECT k, v2 FROM shared_right_distributed_05137) AS r
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'natural_global';
SELECT *
FROM (SELECT k, v1 FROM shared_left_distributed_05137) AS l
GLOBAL NATURAL RIGHT JOIN (SELECT k, v2 FROM shared_right_distributed_05137) AS r
ORDER BY ALL
FORMAT TSVWithNames;

-- USING (a AS b) takes the key from the left table as `a` and from the right one as `b`.
SELECT 'using_alias_initiator';
SELECT *
FROM (SELECT * FROM left_distributed_05137) AS l
RIGHT JOIN (SELECT * FROM right_distributed_05137) AS r USING (k1 AS k2)
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'using_alias_global';
SELECT *
FROM left_distributed_05137 AS l
GLOBAL RIGHT JOIN right_distributed_05137 AS r USING (k1 AS k2)
ORDER BY ALL
FORMAT TSVWithNames;

-- LEFT JOIN is not rewritten and must keep working.
SELECT 'using_left_join_global';
SELECT k, v1, v2
FROM shared_left_distributed_05137 AS l
GLOBAL LEFT JOIN shared_right_distributed_05137 AS r USING (k)
ORDER BY ALL
FORMAT TSVWithNames;

-- Only the left table fans the query out, so the right side may be anything at all.
-- Every result below is paired with the same join forced onto the initiator, which defines the correct rows.
DROP TABLE IF EXISTS right_one_shard_05137;

CREATE TABLE right_one_shard_05137 AS right_local_05137
ENGINE = Distributed('test_shard_localhost', currentDatabase(), right_local_05137);

SELECT 'right_is_local_table_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_distributed_05137) AS l
RIGHT JOIN right_local_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_local_table_global';
SELECT k1, v1, k2, v2
FROM left_distributed_05137 AS l
GLOBAL RIGHT JOIN right_local_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_one_shard_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_distributed_05137) AS l
RIGHT JOIN right_one_shard_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_one_shard_global';
SELECT k1, v1, k2, v2
FROM left_distributed_05137 AS l
GLOBAL RIGHT JOIN right_one_shard_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_subquery_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_distributed_05137) AS l
RIGHT JOIN (SELECT * FROM right_distributed_05137) AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_subquery_global';
SELECT k1, v1, k2, v2
FROM left_distributed_05137 AS l
GLOBAL RIGHT JOIN (SELECT * FROM right_distributed_05137) AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_table_function_initiator';
SELECT k1, v1, n
FROM (SELECT * FROM left_distributed_05137) AS l
RIGHT JOIN (SELECT number AS n FROM numbers(5)) AS r ON l.k1 = r.n
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'right_is_table_function_global';
SELECT k1, v1, n
FROM left_distributed_05137 AS l
GLOBAL RIGHT JOIN (SELECT number AS n FROM numbers(5)) AS r ON l.k1 = r.n
ORDER BY ALL
FORMAT TSVWithNames;

-- ANY, SEMI and ANTI mirror into their LEFT counterparts, so they are rewritten as well.
-- The duplicate key on the left makes ANY and ALL return a different number of rows.
DROP TABLE IF EXISTS dup_left_local_05137;
DROP TABLE IF EXISTS dup_left_distributed_05137;

CREATE TABLE dup_left_local_05137 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE dup_left_distributed_05137 AS dup_left_local_05137
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), dup_left_local_05137);

INSERT INTO dup_left_local_05137 VALUES (1, 'a1'), (1, 'a2'), (2, 'b'), (4, 'd');

SELECT 'any_initiator';
SELECT k2, v2
FROM (SELECT * FROM dup_left_distributed_05137) AS l
RIGHT ANY JOIN right_local_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'any_global';
SELECT k2, v2
FROM dup_left_distributed_05137 AS l
GLOBAL RIGHT ANY JOIN right_local_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'semi_initiator';
SELECT k2, v2
FROM (SELECT * FROM dup_left_distributed_05137) AS l
RIGHT SEMI JOIN right_local_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'semi_global';
SELECT k2, v2
FROM dup_left_distributed_05137 AS l
GLOBAL RIGHT SEMI JOIN right_local_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'anti_initiator';
SELECT k2, v2
FROM (SELECT * FROM dup_left_distributed_05137) AS l
RIGHT ANTI JOIN right_local_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'anti_global';
SELECT k2, v2
FROM dup_left_distributed_05137 AS l
GLOBAL RIGHT ANTI JOIN right_local_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

-- A single shard on the left never duplicates anything, so nothing is rewritten there.
DROP TABLE IF EXISTS left_one_shard_05137;

CREATE TABLE left_one_shard_05137 AS left_local_05137
ENGINE = Distributed('test_shard_localhost', currentDatabase(), left_local_05137);

SELECT 'left_is_one_shard_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_one_shard_05137) AS l
RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'left_is_one_shard_global';
SELECT k1, v1, k2, v2
FROM left_one_shard_05137 AS l
GLOBAL RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

DROP TABLE left_one_shard_05137;

-- A RIGHT join anywhere in the tree emits the preserved rows once per shard, not only at the root.
-- The third table is a superset of the keys, otherwise the outer join filters out exactly the
-- duplicated rows and the test passes while the bug is still there.
DROP TABLE IF EXISTS third_local_05137;
DROP TABLE IF EXISTS third_distributed_05137;

CREATE TABLE third_local_05137 (k3 UInt32, v3 String)
ENGINE = MergeTree
ORDER BY k3;

CREATE TABLE third_distributed_05137 AS third_local_05137
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), third_local_05137);

INSERT INTO third_local_05137 VALUES (1, 'X'), (2, 'Y'), (3, 'Z'), (4, 'W');

SELECT 'nested_right_then_inner_initiator';
SELECT k1, v1, k2, v2, k3, v3
FROM (SELECT * FROM left_distributed_05137) AS l
RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
INNER JOIN third_distributed_05137 AS t ON r.k2 = t.k3
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'nested_right_then_inner_global';
SELECT k1, v1, k2, v2, k3, v3
FROM left_distributed_05137 AS l
GLOBAL RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
GLOBAL INNER JOIN third_distributed_05137 AS t ON r.k2 = t.k3
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'nested_right_then_right_initiator';
SELECT k1, v1, k2, v2, k3, v3
FROM (SELECT * FROM left_distributed_05137) AS l
RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
RIGHT JOIN third_distributed_05137 AS t ON r.k2 = t.k3
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'nested_right_then_right_global';
SELECT k1, v1, k2, v2, k3, v3
FROM left_distributed_05137 AS l
GLOBAL RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
GLOBAL RIGHT JOIN third_distributed_05137 AS t ON r.k2 = t.k3
ORDER BY ALL
FORMAT TSVWithNames;

-- Here the RIGHT join sits at the root and its left side is another join, so the sides cannot be
-- swapped. The join runs on the initiator instead.
SELECT 'nested_inner_then_right_initiator';
SELECT k1, v1, k2, v2, k3, v3
FROM (SELECT * FROM left_distributed_05137) AS l
INNER JOIN third_distributed_05137 AS t ON l.k1 = t.k3
RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'nested_inner_then_right_global';
SELECT k1, v1, k2, v2, k3, v3
FROM left_distributed_05137 AS l
GLOBAL INNER JOIN third_distributed_05137 AS t ON l.k1 = t.k3
GLOBAL RIGHT JOIN right_distributed_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

-- FULL JOIN preserves the broadcast side too, and cannot be repaired by swapping.
SELECT 'full_join_initiator';
SELECT k1, v1, k2, v2
FROM (SELECT * FROM left_distributed_05137) AS l
FULL JOIN right_distributed_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

SELECT 'full_join_global';
SELECT k1, v1, k2, v2
FROM left_distributed_05137 AS l
GLOBAL FULL JOIN right_distributed_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

-- Trees without a RIGHT or FULL join keep running on the shards, so shardNum still varies there.
SELECT 'shard_num_is_untouched_without_right_join';
SELECT DISTINCT shardNum() AS s
FROM left_distributed_05137 AS l
GLOBAL INNER JOIN right_distributed_05137 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

DROP TABLE third_distributed_05137;
DROP TABLE third_local_05137;

DROP TABLE dup_left_distributed_05137;
DROP TABLE dup_left_local_05137;
DROP TABLE right_one_shard_05137;

DROP TABLE shared_left_distributed_05137;
DROP TABLE shared_right_distributed_05137;
DROP TABLE shared_left_local_05137;
DROP TABLE shared_right_local_05137;

DROP TABLE left_distributed_05137;
DROP TABLE right_distributed_05137;
DROP TABLE left_local_05137;
DROP TABLE right_local_05137;
