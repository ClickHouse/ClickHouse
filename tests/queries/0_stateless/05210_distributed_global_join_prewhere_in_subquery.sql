-- Tags: distributed
-- Nested GLOBAL IN inside PREWHERE must not be materialized twice after the
-- PREWHERE is moved into the GLOBAL JOIN broadcast subquery.
-- https://github.com/ClickHouse/ClickHouse/pull/119780

SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS left_local_05210;
DROP TABLE IF EXISTS right_local_05210;
DROP TABLE IF EXISTS left_distributed_05210;
DROP TABLE IF EXISTS right_distributed_05210;

CREATE TABLE left_local_05210 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE right_local_05210 (k2 UInt32, v2 String)
ENGINE = MergeTree
ORDER BY k2;

CREATE TABLE left_distributed_05210 AS left_local_05210
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), left_local_05210);

CREATE TABLE right_distributed_05210 AS right_local_05210
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), right_local_05210);

INSERT INTO left_local_05210 VALUES (1, 'a'), (2, 'b');
INSERT INTO right_local_05210 VALUES (1, 'A'), (2, 'B'), (3, 'C');

SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_distributed_05210 AS l
GLOBAL RIGHT JOIN right_distributed_05210 AS r ON l.k1 = r.k2
PREWHERE l.k1 IN (SELECT k2 FROM right_distributed_05210 WHERE k2 = 1)
GROUP BY ALL
ORDER BY ALL
SETTINGS distributed_product_mode = 'global';

DROP TABLE left_distributed_05210;
DROP TABLE right_distributed_05210;
DROP TABLE left_local_05210;
DROP TABLE right_local_05210;
