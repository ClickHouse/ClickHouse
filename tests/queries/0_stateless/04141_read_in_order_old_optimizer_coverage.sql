-- Test coverage for ReadInOrderOptimizer (used only in InterpreterSelectQuery, i.e. enable_analyzer=0)
-- Exercises: constructor, getInputOrder, getInputOrderImpl, matchSortDescriptionAndKey,
--            getFixedSortingColumns, getFixedPoint

SET enable_analyzer = 0;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_read_in_order_optim;

CREATE TABLE t_read_in_order_optim (a UInt32, b UInt32)
ENGINE = MergeTree()
ORDER BY (a, b)
SETTINGS index_granularity = 4;

INSERT INTO t_read_in_order_optim VALUES (1, 10), (1, 20), (2, 5), (2, 15), (3, 1);

-- Basic ORDER BY matching primary key (ASC direction)
-- Covers constructor, getInputOrder, getInputOrderImpl, matchSortDescriptionAndKey basic path
SELECT a, b FROM t_read_in_order_optim ORDER BY a, b;

-- DESC direction — covers negative read_direction path in getInputOrderImpl
SELECT a, b FROM t_read_in_order_optim ORDER BY a DESC, b DESC;

-- WHERE clause fixes the first key column; ORDER BY the second key column
-- Covers getFixedSortingColumns and getFixedPoint (simple column = literal case)
SELECT a, b FROM t_read_in_order_optim WHERE a = 2 ORDER BY b;

-- WHERE clause fixes the first column, ORDER BY the second in DESC direction
SELECT a, b FROM t_read_in_order_optim WHERE a = 1 ORDER BY b DESC;

-- Both WHERE and PREWHERE: covers the combined-condition branch in getFixedSortingColumns
SELECT a, b FROM t_read_in_order_optim PREWHERE a = 1 WHERE b > 5 ORDER BY b;

-- Only PREWHERE (no WHERE): covers the prewhere-only branch in getFixedSortingColumns
SELECT a, b FROM t_read_in_order_optim PREWHERE a = 2 ORDER BY b;

-- ORDER BY with a monotonic function on a key column
-- Covers the monotonic-function path in matchSortDescriptionAndKey
SELECT a FROM t_read_in_order_optim ORDER BY toUInt64(a) LIMIT 3;

DROP TABLE t_read_in_order_optim;
