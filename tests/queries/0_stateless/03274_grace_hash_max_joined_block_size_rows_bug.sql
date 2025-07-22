DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t0 SELECT number from numbers(20);

CREATE TABLE t1 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t1 SELECT number from numbers(5, 20);

SET max_joined_block_size_rows = 1;
SET grace_hash_join_initial_buckets = 2;
SET join_algorithm = 'grace_hash';

SELECT sum(x), count() FROM t0 JOIN t1 USING x;

