DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
SELECT t0.c0 FROM t0 JOIN t0 tx ON t0.c0 = tx.c0 WHERE tx._part_offset = 1 AND randomFixedString(5) = tx._part SETTINGS query_plan_use_logical_join_step = 0, use_join_disjunctions_push_down = 1, enable_analyzer = 1;
DROP TABLE t0;
