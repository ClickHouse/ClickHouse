-- The block squashing after a widened join exists for `parallel_hash`, which emits one slot's fragment
-- of every probe block. `partitioned_hash` joins probe blocks whole and caps its output at
-- `max_joined_block_size_rows`, so its blocks now pass through the squashing untouched, as `hash`'s
-- do. The spilling wrapper decides at run time: the in-memory join passes through, a switch to grace
-- squashes again, because the grace join emits one bucket's share of each probe block.
SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET grace_hash_join_initial_buckets = 1;
SET grace_hash_join_max_buckets = 1024;
SET max_threads = 4;
-- Every build here is tiny; the parallel fill (and with it the squashing) needs the threshold off.
SET parallel_hash_join_threshold = 0;
SET max_joined_block_size_rows = 5;

DROP TABLE IF EXISTS t_sq_left;
DROP TABLE IF EXISTS t_sq_right;

CREATE TABLE t_sq_left (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 2, number FROM numbers(10);
CREATE TABLE t_sq_right (a UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 2 FROM numbers(10);

-- Five matches per left row, one left row per block from the cap: every joined block holds exactly one
-- left row's matches, so blocks larger than 5 rows can only come from squashing.
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

SELECT '-- the squashing stays in the pipeline for the run-time decision';
SELECT countIf(explain LIKE '%SimpleSquashingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM t_sq_left AS l JOIN t_sq_right AS r ON l.a = r.a SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 0);

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05143 grace'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_sq_left;
DROP TABLE t_sq_right;
