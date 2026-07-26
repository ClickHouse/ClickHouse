-- When the plan relies on a join preserving the order of its left input (read-in-order
-- through the join), `SpillingHashJoin` must not switch to `GraceHashJoin`: spilling
-- scatters rows into buckets by hash and the query would return wrongly ordered rows.
--
-- The threshold below is deliberately tiny, so a `SpillingHashJoin` that is free to spill
-- certainly would. Pinned by the read-in-order optimisation it must stay in memory instead.

DROP TABLE IF EXISTS t_keep_order_left;
DROP TABLE IF EXISTS t_keep_order_right;

CREATE TABLE t_keep_order_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_keep_order_right (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_keep_order_left SELECT number, number FROM numbers(100000);
INSERT INTO t_keep_order_right SELECT number FROM numbers(100000);

SELECT l.k
FROM t_keep_order_left AS l
INNER JOIN t_keep_order_right AS r ON l.k = r.k
ORDER BY l.k
LIMIT 5
SETTINGS join_algorithm = 'hash',
         optimize_read_in_order = 1,
         max_bytes_ratio_before_external_join = 0,
         max_bytes_before_external_join = 1,
         log_comment = '04648_keep_left_order';

SYSTEM FLUSH LOGS query_log;

-- The join was pinned to the in-memory algorithm, so it never spilled despite the 1-byte
-- threshold.
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS spills
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04648_keep_left_order'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Without a plan that needs the left order the same join is free to spill, which is what the
-- tiny threshold asks for. Check that the result is still correct in that state.
SELECT count(), min(l.k), max(l.k)
FROM t_keep_order_left AS l
INNER JOIN t_keep_order_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash',
         max_bytes_ratio_before_external_join = 0,
         max_bytes_before_external_join = 1;

DROP TABLE t_keep_order_left;
DROP TABLE t_keep_order_right;
