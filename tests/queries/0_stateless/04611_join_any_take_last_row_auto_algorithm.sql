-- Test that join_any_take_last_row is honored with join_algorithm='auto'
-- for both the JoinSwitcher path and the HashJoin path (multi-disjunct ANY JOIN).
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

SET enable_analyzer = 1, query_plan_join_swap_table = 0, join_algorithm = 'auto';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

CREATE TABLE t1 (k UInt32, v UInt32) ENGINE MergeTree ORDER BY (k, v);
INSERT INTO t1 VALUES (1, 42);
CREATE TABLE t2 (k UInt32, v UInt32) ENGINE MergeTree ORDER BY (k, v);
INSERT INTO t2 VALUES (1, 7), (1, 8);

-- Single-disjunct ANY JOIN (JoinSwitcher wraps HashJoin).
SELECT 's0', t2.v FROM t1 ANY LEFT JOIN t2 ON t1.k = t2.k ORDER BY t1.k SETTINGS join_any_take_last_row = 0;
SELECT 's1', t2.v FROM t1 ANY LEFT JOIN t2 ON t1.k = t2.k ORDER BY t1.k SETTINGS join_any_take_last_row = 1;

-- Multi-disjunct ANY JOIN forces plain HashJoin inside the AUTO branch.
SELECT 'm0', t2.v FROM t1 ANY LEFT JOIN t2 ON t1.k = t2.k OR t1.v = t2.v ORDER BY t1.k SETTINGS join_any_take_last_row = 0;
SELECT 'm1', t2.v FROM t1 ANY LEFT JOIN t2 ON t1.k = t2.k OR t1.v = t2.v ORDER BY t1.k SETTINGS join_any_take_last_row = 1;

DROP TABLE t1;
DROP TABLE t2;
