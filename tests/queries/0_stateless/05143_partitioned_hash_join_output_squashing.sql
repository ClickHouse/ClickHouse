-- The squashing step after a join exists for the parallel `hash` layout. That layout's output splits
-- each probe block into one piece per hash table. `partitioned_hash` joins each probe block whole
-- and caps its output at `max_joined_block_size_rows`. Its blocks are therefore not squashed, as
-- the serial `hash` layout's are not.
SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET max_threads = 4;
-- Every build here is tiny; the parallel build (and with it the squashing) needs the threshold off.
SET parallel_hash_join_threshold = 0;
SET max_joined_block_size_rows = 5;

DROP TABLE IF EXISTS t_sq_left;
DROP TABLE IF EXISTS t_sq_right;

CREATE TABLE t_sq_left (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 2, number FROM numbers(10);
CREATE TABLE t_sq_right (a UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 2 FROM numbers(10);

-- Five matches per left row. The cap yields one left row per block. Every joined block therefore
-- holds exactly one left row's matches. Blocks larger than 5 rows can only come from squashing.
SELECT '-- hash: no squashing after the join';
SELECT max(bs) <= 5, count() FROM (SELECT blockSize() AS bs, * FROM t_sq_left AS l JOIN t_sq_right AS r ON l.a = r.a) SETTINGS join_algorithm = 'hash';

SELECT '-- partitioned_hash, bare join: the blocks pass through';
SELECT max(bs) <= 5, count() FROM (SELECT blockSize() AS bs, * FROM t_sq_left AS l JOIN t_sq_right AS r ON l.a = r.a)
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0;
SELECT max(bs) <= 5, count() FROM (SELECT blockSize() AS bs, * FROM t_sq_left AS l LEFT JOIN t_sq_right AS r ON l.a = r.a)
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0;
SELECT max(bs) <= 5, count() FROM (SELECT blockSize() AS bs, * FROM t_sq_left AS l RIGHT JOIN t_sq_right AS r ON l.a = r.a)
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0;
SELECT max(bs) <= 5, count() FROM (SELECT blockSize() AS bs, * FROM t_sq_left AS l FULL JOIN t_sq_right AS r ON l.a = r.a)
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0;

DROP TABLE t_sq_left;
DROP TABLE t_sq_right;
