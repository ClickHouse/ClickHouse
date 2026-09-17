-- The squashing step after a join exists for the parallel `hash` layout. That layout's output splits
-- each probe block into one piece per hash table. `partitioned_hash` joins each probe block whole
-- and caps its output at `max_joined_block_size_rows`. Its blocks are therefore not squashed, as
-- the serial `hash` layout's are not.
--
-- Inside `SpillingHashJoin` the decision is made at run time: while the join stays in memory the blocks
-- pass through. After a switch to `GraceHashJoin` they are squashed again, because grace emits one
-- bucket's part of each probe block.
SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET grace_hash_join_initial_buckets = 1;
SET grace_hash_join_max_buckets = 1024;
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

SELECT '-- partitioned_hash inside the spilling wrapper, build kept in memory: still pass through';
SELECT max(bs) <= 5, count() FROM (SELECT blockSize() AS bs, * FROM t_sq_left AS l JOIN t_sq_right AS r ON l.a = r.a)
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 1000000000;
SELECT max(bs) <= 5, count() FROM (SELECT blockSize() AS bs, * FROM t_sq_left AS l FULL JOIN t_sq_right AS r ON l.a = r.a)
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 1000000000;

SELECT '-- switched to grace during the build: the bucket output is squashed again (one stream, so one block)';
SELECT max(bs) > 5, count() FROM (SELECT blockSize() AS bs, * FROM t_sq_left AS l JOIN t_sq_right AS r ON l.a = r.a)
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 1, max_threads = 1, log_comment = '05143 grace';

SELECT '-- the bare join decides at plan time and has no squashing; the wrapper keeps it for the run-time decision';
SELECT countIf(explain LIKE '%SimpleSquashingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM t_sq_left AS l JOIN t_sq_right AS r ON l.a = r.a SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0);
SELECT countIf(explain LIKE '%SimpleSquashingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM t_sq_left AS l JOIN t_sq_right AS r ON l.a = r.a SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 1000000000);

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05143 grace'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_sq_left;
DROP TABLE t_sq_right;
