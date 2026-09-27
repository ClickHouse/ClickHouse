-- `ConstantJoin` spills the stored right-side blocks to a temporary block stream when the join size limits
-- are approached, and the non-joined output (`RIGHT`/`FULL` join with a constant-false predicate) must then
-- stream every spilled row back exactly once.

SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET join_use_nulls = 0;
SET max_block_size = 999;

DROP TABLE IF EXISTS t_right_spill;
CREATE TABLE t_right_spill (n UInt64, s String) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_right_spill SELECT number, repeat('y', 100) FROM numbers(10000);
INSERT INTO t_right_spill SELECT number + 10000, repeat('z', 100) FROM numbers(10000);

SELECT 'right_join_spill_by_rows';
SELECT count(), uniqExact(n), sum(n), min(length(s)), max(l.x)
FROM (SELECT 1 AS x) AS l
RIGHT JOIN t_right_spill AS r ON 0
SETTINGS max_rows_in_join = 111, log_comment = '04546_spill_by_rows';

SELECT 'right_join_spill_by_bytes';
SELECT count(), uniqExact(n), sum(n), min(length(s)), max(l.x)
FROM (SELECT 1 AS x) AS l
RIGHT JOIN t_right_spill AS r ON 0
SETTINGS max_bytes_in_join = 100000, log_comment = '04546_spill_by_bytes';

SELECT 'full_join_spill_by_rows';
SELECT count(), uniqExact(n), sum(n), min(length(s)), sum(l.x)
FROM (SELECT 1 AS x) AS l
FULL JOIN t_right_spill AS r ON 0
SETTINGS max_rows_in_join = 111, log_comment = '04546_spill_by_rows_full';

-- Make sure the right side was actually spilled to disk.
SYSTEM FLUSH LOGS query_log;
SELECT 'spilled';
SELECT log_comment, max(ProfileEvents['ExternalJoinWritePart']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE '04546_spill%' AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_right_spill;
