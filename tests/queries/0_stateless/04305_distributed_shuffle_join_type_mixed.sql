-- Regression test: shuffle-join key types differ on left and right side.
-- Without casting to a common supertype, the scatter step on each side uses different
-- hashing because of different physical types, so matching rows are routed to different
-- buckets and the join silently drops them.

-- Pin a nonzero max_rows_to_group_by to keep the count single-node; the test targets the shuffle join.
SET max_rows_to_group_by = 1000000;

DROP TABLE IF EXISTS t_shuffle_join_left;
DROP TABLE IF EXISTS t_shuffle_join_right;

CREATE TABLE t_shuffle_join_left  (k Int8,  v UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_shuffle_join_right (k Int64, v UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_shuffle_join_left  SELECT (number % 100) - 50, number FROM numbers(10000);
INSERT INTO t_shuffle_join_right SELECT (number % 100) - 50, number * 2 FROM numbers(10000);

SELECT '-- Local';
SELECT count() FROM t_shuffle_join_left AS l JOIN t_shuffle_join_right AS r ON l.k = r.k;

SELECT '-- Distributed plan';
EXPLAIN SELECT count() FROM t_shuffle_join_left AS l JOIN t_shuffle_join_right AS r ON l.k = r.k
SETTINGS
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0,
    enable_join_runtime_filters = 0;

SELECT '-- Distributed';
SELECT count() FROM t_shuffle_join_left AS l JOIN t_shuffle_join_right AS r ON l.k = r.k
SETTINGS
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0,
    enable_join_runtime_filters = 0;

DROP TABLE t_shuffle_join_left;
DROP TABLE t_shuffle_join_right;
