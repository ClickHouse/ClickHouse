-- Tests that `join_any_take_last_row` is honored when the join goes through `SpillingHashJoin`
-- (selected when `max_bytes_before_external_join` / `max_bytes_ratio_before_external_join` enable auto-spilling).
-- Previously the setting was hardcoded to `false` inside `SpillingHashJoin`, so `a1` and `a2` produced
-- identical results.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
SET query_plan_join_swap_table = 0;
CREATE TABLE t1 (k UInt32, v UInt32) ENGINE MergeTree ORDER BY (k, v);
INSERT INTO t1 VALUES (1, 42), (1, 43);


CREATE TABLE t2 (k UInt32, v UInt32, just_for_size String) ENGINE MergeTree ORDER BY (k, v);
INSERT INTO t2 VALUES (1, 42, ''), (1, 43, '');
INSERT INTO t2 SELECT number + 2 AS k, number + 2 AS v, randomPrintableASCII(1000) AS just_for_size FROM system.numbers LIMIT 20000;

SET enable_analyzer = 1; -- The old analyzer didn't pass `join_any_take_last_row` to the joins, so we need the analyzer
-- Auto-spilling path (SpillingHashJoin) with spill.
SET max_bytes_before_external_join = 10000000, max_bytes_ratio_before_external_join = 0.8;
SELECT 'a1', * FROM t1 ANY JOIN t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 0;
SELECT 'a2', * FROM t1 ANY JOIN t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 1;
SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND databases != ['system']
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
ORDER BY event_time_microseconds DESC
LIMIT 2;
-- Auto-spilling path (SpillingHashJoin) without spill.
SET max_bytes_before_external_join = 10000000000000, max_bytes_ratio_before_external_join = 0.8;
SELECT 'b1', * FROM t1 ANY JOIN t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 0;
SELECT 'b2', * FROM t1 ANY JOIN t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 1;

-- In-memory path (plain HashJoin), for comparison.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SELECT 'c1', * FROM t1 ANY JOIN t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 0;
SELECT 'c2', * FROM t1 ANY JOIN t2 ON t1.k = t2.k SETTINGS join_any_take_last_row = 1;
SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND databases != ['system']
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
ORDER BY event_time_microseconds DESC
LIMIT 6;

DROP TABLE t1;
DROP TABLE t2;
