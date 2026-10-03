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
