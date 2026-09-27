-- Tags: distributed

SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS left_local_05142;
DROP TABLE IF EXISTS right_local_05142;
DROP TABLE IF EXISTS left_distributed_05142;
DROP TABLE IF EXISTS right_distributed_05142;
DROP TABLE IF EXISTS left_buffer_05142;

CREATE TABLE left_local_05142 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE right_local_05142 (k2 UInt32, v2 String)
ENGINE = MergeTree
ORDER BY k2;

CREATE TABLE left_distributed_05142 AS left_local_05142
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), left_local_05142);

CREATE TABLE right_distributed_05142 AS right_local_05142
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), right_local_05142);

CREATE TABLE left_buffer_05142 AS left_local_05142
ENGINE = Buffer(currentDatabase(), left_distributed_05142, 1, 1, 1, 1, 1, 1, 1);

INSERT INTO left_local_05142 VALUES (1, 'a'), (2, 'b'), (4, 'd');
INSERT INTO right_local_05142 VALUES (1, 'A'), (2, 'B'), (3, 'C');

SELECT 'initiator';
SELECT *
FROM (SELECT * FROM left_buffer_05142) AS l
RIGHT JOIN (SELECT * FROM right_distributed_05142) AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

-- A `Buffer` whose destination is a sharded `Distributed` table still fans the query out
-- across shards, so the rewrite must look through it the same way as a bare `Distributed` table.
SELECT 'buffer_global';
SELECT *
FROM left_buffer_05142 AS l
GLOBAL RIGHT JOIN right_distributed_05142 AS r ON l.k1 = r.k2
ORDER BY ALL
FORMAT TSVWithNames;

DROP TABLE left_buffer_05142;
DROP TABLE left_distributed_05142;
DROP TABLE right_distributed_05142;
DROP TABLE left_local_05142;
DROP TABLE right_local_05142;
