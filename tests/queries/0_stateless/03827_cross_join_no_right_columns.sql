-- Test that cross join works correctly when no columns from the right side are needed.
-- This used to cause std::out_of_range in HashJoin::getTotalRowCount() because
-- columns_info.columns was empty but .at(0) was used to get the row count.
-- The bug triggers when PREWHERE consumes ALL columns from the right table,
-- causing the right side of the join to pass zero columns to the join.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t2 (a UInt64, b String) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 VALUES (1), (2), (3);
INSERT INTO t2 VALUES (1, 'x'), (2, 'y');
SET enable_analyzer = 1;
-- PREWHERE references all columns from t2, so after PREWHERE pushdown
-- the right side of the join has zero columns in its header.
SELECT count() FROM t1, t2 PREWHERE a > 0 AND b != ''
SETTINGS query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false';

DROP TABLE t1;
DROP TABLE t2;

-- The same when the zero-column right side spans several blocks, where the row count of a block is
-- needed and `Block::rows` reports 0 for it.
--
-- `a > 0` is deliberate: an always-true comparison such as `a >= 0` on an unsigned column is
-- constant folded away, the right side then keeps the `b.size` subcolumn and the zero-column
-- representation under test is never produced.
DROP TABLE IF EXISTS t1_big;
DROP TABLE IF EXISTS t2_big;

CREATE TABLE t1_big (x UInt64) ENGINE = MergeTree ORDER BY x;
-- `index_granularity` is pinned so the right side arrives in 1000-row blocks: the row accounting
-- below has to reach the limit exactly between two blocks, which is where a zero-column block
-- would otherwise be spilled and its rows lost.
CREATE TABLE t2_big (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1000, index_granularity_bytes = 0;

INSERT INTO t1_big VALUES (1), (2), (3);
INSERT INTO t2_big SELECT number + 1, toString(number) FROM numbers(100000);

-- The assertion is on the right pre-join input header, which is empty exactly when the right side
-- feeds zero columns into the join. The join output header is not a substitute: it says nothing
-- about which side the surviving column comes from.
--
-- The join order has to be pinned identically for the assertion and for the query below it: the
-- test runner randomizes it, and with t2_big on the left the build side becomes the single block
-- t1_big, which no longer covers a zero-column right side spanning several blocks.
SELECT countIf(explain ILIKE '%#1 Empty header%') = 1 FROM (
    EXPLAIN input_headers = 1 SELECT count() FROM t1_big, t2_big PREWHERE a > 0 AND b != ''
    SETTINGS query_plan_remove_unused_columns = 1,
             query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false'
);

SELECT count() FROM t1_big, t2_big PREWHERE a > 0 AND b != ''
SETTINGS max_block_size = 1000, max_threads = 1, query_plan_remove_unused_columns = 1,
         query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false';

-- A zero-column right block is never spilled: a temporary stream would serialize `Block::rows` == 0
-- and the read side would reconstruct 0 rows for it, silently undercounting the cross product. Its
-- rows are accounted for in memory instead, so a row limit below the size of the right side is
-- reported. Spilling such a block instead returns 15000 rather than raising the limit.
SELECT count() FROM t1_big, t2_big PREWHERE a > 0 AND b != ''
SETTINGS max_rows_in_join = 5000, max_block_size = 1000, max_threads = 1, query_plan_remove_unused_columns = 1,
         query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

DROP TABLE t1_big;
DROP TABLE t2_big;
