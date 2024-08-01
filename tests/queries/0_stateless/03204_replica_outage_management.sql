-- Tags: distributed, no-parallel

CREATE DATABASE IF NOT EXISTS test_03204;
USE test_03204;

DROP TABLE IF EXISTS t1_shard;
DROP TABLE IF EXISTS t2_shard;
DROP TABLE IF EXISTS t1_distr;
DROP TABLE IF EXISTS t2_distr;

-- Create the shard tables
CREATE TABLE t1_shard (id Int32, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2_shard (id Int32, value String) ENGINE = MergeTree ORDER BY id;

-- Create the distributed tables
CREATE TABLE t1_distr AS t1_shard ENGINE = Distributed(test_cluster_two_shards_localhost, test_03204, t1_shard, id);
CREATE TABLE t2_distr AS t2_shard ENGINE = Distributed(test_cluster_two_shards_localhost, test_03204, t2_shard, id);

-- Insert some data into the shard tables
INSERT INTO t1_shard VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t2_shard VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Set the distributed product mode to allow global subqueries
SET distributed_product_mode = 'global';

-- Simple distributed query to verify setup
SELECT d0.id, d0.value
FROM t1_distr d0
WHERE d0.id IN
(
    SELECT d1.id
    FROM t1_distr AS d1
    INNER JOIN t2_distr AS d2 ON d1.id = d2.id
    WHERE d1.id > 0
    ORDER BY d1.id
);

SELECT d0.id, d0.value
FROM t1_distr d0
JOIN (
    SELECT d1.id, d1.value
    FROM t1_distr AS d1
    INNER JOIN t2_distr AS d2 ON d1.id = d2.id
    WHERE d1.id > 0
    ORDER BY d1.id
) s0 USING (id, value);

-- Cleanup
DROP TABLE t1_shard;
DROP TABLE t2_shard;
DROP TABLE t1_distr;
DROP TABLE t2_distr;
DROP DATABASE test_03204;
