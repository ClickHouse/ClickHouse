DROP TABLE IF EXISTS t1_04498;
DROP TABLE IF EXISTS t2_04498;

CREATE TABLE t1_04498 (x UInt32, y UInt64) ENGINE = MergeTree ORDER BY (x, y);
CREATE TABLE t2_04498 (x UInt32, y UInt64) ENGINE = MergeTree ORDER BY (x, y);

-- Freeze merges so the two inserts below stay as two separate right-side parts.
-- The bug needs the right side split into more than one block; a background merge
-- collapsing them would let partial_merge read a single block and pass spuriously.
SYSTEM STOP MERGES t2_04498;

INSERT INTO t1_04498 VALUES (0,0),(1,10),(2,20),(3,30),(4,40);
INSERT INTO t2_04498 VALUES (2,21),(2,22),(4,41);
INSERT INTO t2_04498 VALUES (0,0),(4,42),(5,50);

SET optimize_distinct_in_order = 1;

-- read-in-order-through-join is a local-coordinator plan optimization. Under parallel replicas the
-- MergeTree read is remote (ReadFromRemoteParallelReplicas), so the sorted consumer is never planted
-- on the local plan and the InOrder plan probes below would read 0 instead of the expected 1. The
-- ParallelReplicas CI variant enables parallel replicas in the default profile, so pin it off here.
SET enable_parallel_replicas = 0;

-- Pin the whole read-in-order trio on every DISTINCT query below. buildInputOrderInfo(DistinctStep)
-- only traverses the join when query_plan_read_in_order_through_join is on (optimizeReadInOrder.cpp),
-- and the sorted consumer is only planted when optimize_read_in_order / query_plan_read_in_order are on.
-- The stateless runner randomizes all three; if any lands at 0 the sorted consumer is never planted,
-- so the query returns the correct 6 rows even on an unfixed binary and the guard goes blind.

SET join_algorithm = 'partial_merge';

-- intDiv(t2.y, 2147483647) maps every t2 row to key 0, so this INNER JOIN matches the single
-- t1 row (0,0) against all 6 distinct t2 rows: DISTINCT must return exactly these 6 rows.
SELECT DISTINCT t1_04498.*, t2_04498.*
FROM t1_04498 INNER JOIN t2_04498 ON intDiv(t2_04498.y, 2147483647) = toUInt64(t1_04498.x)
ORDER BY ALL
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;

SELECT count() FROM (
    SELECT DISTINCT t1_04498.*, t2_04498.*
    FROM t1_04498 INNER JOIN t2_04498 ON intDiv(t2_04498.y, 2147483647) = toUInt64(t1_04498.x)
) SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;

SET join_algorithm = 'prefer_partial_merge';
SELECT count() FROM (
    SELECT DISTINCT t1_04498.*, t2_04498.*
    FROM t1_04498 INNER JOIN t2_04498 ON intDiv(t2_04498.y, 2147483647) = toUInt64(t1_04498.x)
) SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;

-- full_sorting_merge re-sorts the left side by the join key, so it must not carry the left
-- sort property either. It must return the same 6 distinct rows.
SET join_algorithm = 'full_sorting_merge';
SELECT count() FROM (
    SELECT DISTINCT t1_04498.*, t2_04498.*
    FROM t1_04498 INNER JOIN t2_04498 ON intDiv(t2_04498.y, 2147483647) = toUInt64(t1_04498.x)
) SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;

DROP TABLE t1_04498;
DROP TABLE t2_04498;

-- parallel_hash (ConcurrentHashJoin) also breaks the left order for some key shapes. It builds
-- its inner HashJoins with two-level maps, but chooseMethod leaves a single key materializing to
-- 1 byte (key8) or 2 bytes (key16) single-level; wider keys, including string and fixed-string
-- ones, get a two-level variant. For the single-level shapes joinBlock scatters the left block
-- across slots and emits slot 0, then 1, ..., so equal
-- left-key values stop being contiguous. It must not carry the left sort property in that case.
DROP TABLE IF EXISTS t3_04498;
DROP TABLE IF EXISTS t4_04498;
CREATE TABLE t3_04498 (a UInt32, j UInt8) ENGINE = MergeTree ORDER BY (a, j);
CREATE TABLE t4_04498 (j UInt8, v UInt64) ENGINE = MergeTree ORDER BY j;
INSERT INTO t3_04498 SELECT intDiv(number, 8)::UInt32, (number % 8)::UInt8 FROM numbers(64);
INSERT INTO t4_04498 SELECT (number % 8)::UInt8, number FROM numbers(8);

SET join_algorithm = 'parallel_hash';
SET max_threads = 8;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET query_plan_join_swap_table = 0;

-- j is UInt8 -> key8 single-level map -> the left block is scattered. optimize_aggregation_in_order
-- must not be applied on top of the join. Every a in 0..7 matches exactly one right row, so the
-- correct count is 8 per group. Before the fix the sorted consumer saw a=[0..7, 0..7, ...] and
-- closed the first 7 groups after one row each (count 1), lumping the remaining 57 rows into a=7.
-- Pin the whole read-in-order trio (the stateless runner randomizes all three): with
-- query_plan_read_in_order = 0 the sorted consumer is never planted, so the query is correct even
-- on an unfixed binary and the guard goes blind.
SET optimize_aggregation_in_order = 1;
SELECT a, count() FROM t3_04498 LEFT ALL JOIN t4_04498 ON t3_04498.j = t4_04498.j GROUP BY a ORDER BY a
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;

-- The read-in-order-through-join propagation must NOT reach the MergeTree read for this
-- single-level (key8) parallel_hash join: no left read is done InOrder (assertion result 0).
SELECT count() FROM (
    EXPLAIN PLAN
    SELECT a, count() FROM t3_04498 LEFT ALL JOIN t4_04498 ON t3_04498.j = t4_04498.j GROUP BY a ORDER BY a
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1
) WHERE explain ILIKE '%Read type: InOrder%';

-- Single-slot fast path: with max_threads = 1 the join has one slot, so dispatchBlock
-- short-circuits and joinBlock passes the left block through unscattered even for a
-- single-level (key8) map. Order IS preserved, so read-in-order must fire again (InOrder
-- assertion result >= 1) and aggregation-in-order on top of it must still be correct
-- (8 per group). Before the single-slot fix preservesLeftBlockOrder() returned false here,
-- disabling the optimization even though the join preserves order.
SELECT a, count() FROM t3_04498 LEFT ALL JOIN t4_04498 ON t3_04498.j = t4_04498.j GROUP BY a ORDER BY a
SETTINGS max_threads = 1, optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;

SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT a, count() FROM t3_04498 LEFT ALL JOIN t4_04498 ON t3_04498.j = t4_04498.j GROUP BY a ORDER BY a
    SETTINGS max_threads = 1, optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1
) WHERE explain ILIKE '%Read type: InOrder%';

-- With a wide (UInt64 -> two-level) key the map is shared and joinBlock does NOT scatter, so
-- left order is preserved and read-in-order legitimately still fires (assertion result >= 1).
-- This proves the fix is precise (order preservation keyed on the map type), not a blanket disable.
-- The trio is pinned here too: with query_plan_read_in_order = 0 no read is done InOrder and this
-- `> 0` assertion would fail spuriously.
DROP TABLE IF EXISTS t5_04498;
DROP TABLE IF EXISTS t6_04498;
CREATE TABLE t5_04498 (a UInt32, j UInt64) ENGINE = MergeTree ORDER BY (a, j);
CREATE TABLE t6_04498 (j UInt64, v UInt64) ENGINE = MergeTree ORDER BY j;
INSERT INTO t5_04498 SELECT intDiv(number, 8)::UInt32, (number % 8)::UInt64 FROM numbers(64);
INSERT INTO t6_04498 SELECT (number % 8)::UInt64, number FROM numbers(8);
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT a, count() FROM t5_04498 LEFT ALL JOIN t6_04498 ON t5_04498.j = t6_04498.j GROUP BY a ORDER BY a
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1
) WHERE explain ILIKE '%Read type: InOrder%';

DROP TABLE t3_04498;
DROP TABLE t4_04498;
DROP TABLE t5_04498;
DROP TABLE t6_04498;
