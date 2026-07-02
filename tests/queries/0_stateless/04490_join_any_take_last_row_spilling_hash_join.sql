-- Tests that `join_any_take_last_row` is honored when the join goes through `SpillingHashJoin`
-- (selected when `max_bytes_before_external_join` / `max_bytes_ratio_before_external_join` enable auto-spilling).
-- Previously the setting was hardcoded to `false` inside `SpillingHashJoin`, so `a1` and `a2` produced
-- identical results.

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (k UInt32, v UInt32) ENGINE MergeTree ORDER BY k;
INSERT INTO t1 VALUES (1, 42), (1, 43);

-- Auto-spilling path (SpillingHashJoin).
SET max_bytes_before_external_join = 1000000000, max_bytes_ratio_before_external_join = 0.8;
SELECT 'a1', * FROM t1 ANY JOIN t1 AS t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 0;
SELECT 'a2', * FROM t1 ANY JOIN t1 AS t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 1;

-- In-memory path (plain HashJoin), for comparison.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SELECT 'b1', * FROM t1 ANY JOIN t1 AS t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 0;
SELECT 'b2', * FROM t1 ANY JOIN t1 AS t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 1;

DROP TABLE t1;
