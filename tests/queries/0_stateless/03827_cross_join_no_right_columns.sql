-- Test that cross join works correctly when no columns from the right side are needed.
-- This used to cause std::out_of_range in HashJoin::getTotalRowCount() because
-- columns_info.columns was empty but .at(0) was used to get the row count.
-- The bug triggers when PREWHERE consumes ALL columns from the right table,
-- causing the right side of the join to pass zero columns to HashJoin.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t2 (a UInt64, b String) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 VALUES (1), (2), (3);
INSERT INTO t2 VALUES (1, 'x'), (2, 'y');
SET enable_analyzer = 1;
-- PREWHERE references all columns from t2, so after PREWHERE pushdown
-- the right side of the join has zero columns in its header.
SELECT count() FROM t1, t2 PREWHERE a > 0 AND b != '';

DROP TABLE t1;
DROP TABLE t2;

-- A zero-column right block also reached the cross-join block compression path in
-- HashJoin::addBlockToJoin, where `chassert(rows == block.rows())` aborted: rows is taken from
-- the selector (the real row count) but Block::rows() is 0 for a zero-column block. The branch is
-- entered once getTotalRowCount() crosses the threshold, i.e. from the second right block on, so
-- the right side must span more than one block (small max_block_size below).
DROP TABLE IF EXISTS t1_big;
DROP TABLE IF EXISTS t2_big;

CREATE TABLE t1_big (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t2_big (a UInt64, b String) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1_big VALUES (1), (2), (3);
INSERT INTO t2_big SELECT number, toString(number) FROM numbers(100000);

-- compress by rows
SELECT count() FROM t1_big, t2_big PREWHERE a >= 0 AND b != ''
SETTINGS cross_join_min_rows_to_compress = 1, max_block_size = 1000, max_threads = 1;
-- compress by bytes
SELECT count() FROM t1_big, t2_big PREWHERE a >= 0 AND b != ''
SETTINGS cross_join_min_bytes_to_compress = 1, max_block_size = 1000, max_threads = 1;

-- A zero-column right block also reached the cross-join spill-to-disk path. There the temporary stream
-- serializes Block::rows() == 0 (the count lives only in the selector) and Block::empty() stops the read
-- side at the first spilled block, so every spilled zero-column right block contributed 0 rows and the
-- cross product was silently undercounted. A small max_rows_in_join keeps the first blocks in memory then
-- routes later zero-column blocks through the spill path, so the result must still be the full product.
SELECT count() FROM t1_big, t2_big PREWHERE a >= 0 AND b != ''
SETTINGS max_rows_in_join = 5000, max_block_size = 1000, max_threads = 1,
         cross_join_min_rows_to_compress = 0, cross_join_min_bytes_to_compress = 0;

DROP TABLE t1_big;
DROP TABLE t2_big;
