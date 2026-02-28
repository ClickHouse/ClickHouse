-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/85459
-- HashJoin is created at plan-building time with a snapshot of the right-side header.
-- Query plan optimizations (outer-to-inner join conversion with join_use_nulls,
-- filter push-down) can change the right child's output header afterwards,
-- causing checkBlockStructure to fail in HashJoin::addBlockToJoin.
-- The fix re-clones the join in JoinStep::updatePipeline when the header diverges.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (a UInt64, b String) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t_right (a UInt64, c UInt64) ENGINE = MergeTree() ORDER BY a;

INSERT INTO t_left VALUES (1, 'x'), (2, 'y'), (3, 'z');
INSERT INTO t_right VALUES (1, 10), (2, 20);

SET enable_analyzer = 1;
SET join_use_nulls = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

-- LEFT JOIN converted to INNER because of filter on right column.
-- With join_use_nulls, the right header was originally Nullable but after
-- outer-to-inner conversion, the optimizer may push a non-null filter down,
-- changing the right pipeline header. This used to cause a LOGICAL_ERROR.
SELECT t_left.a, t_left.b, t_right.c
FROM t_left
LEFT JOIN t_right ON t_left.a = t_right.a
WHERE t_right.c > 0
ORDER BY t_left.a;

-- Verify the join was actually converted to INNER.
SELECT trim(explain)
FROM (
    EXPLAIN PLAN actions = 1
    SELECT t_left.a, t_left.b, t_right.c
    FROM t_left
    LEFT JOIN t_right ON t_left.a = t_right.a
    WHERE t_right.c > 0
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT');

-- Same scenario with multiple right-side columns.
DROP TABLE IF EXISTS t_right2;
CREATE TABLE t_right2 (a UInt64, c UInt64, d String) ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_right2 VALUES (1, 10, 'a'), (2, 20, 'b'), (4, 40, 'd');

SELECT t_left.a, t_right2.c, t_right2.d
FROM t_left
LEFT JOIN t_right2 ON t_left.a = t_right2.a
WHERE t_right2.c IS NOT NULL
ORDER BY t_left.a;

-- FULL JOIN converted to RIGHT JOIN by filter on left column.
SELECT t_left.a, t_right2.c
FROM t_left
FULL JOIN t_right2 ON t_left.a = t_right2.a
WHERE t_left.a IS NOT NULL
ORDER BY t_left.a;

-- Multiple joins: both converted.
SELECT t_left.a, t_right.c, t_right2.d
FROM t_left
LEFT JOIN t_right ON t_left.a = t_right.a
LEFT JOIN t_right2 ON t_left.a = t_right2.a
WHERE t_right.c > 0 AND t_right2.d != ''
ORDER BY t_left.a;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_right2;
