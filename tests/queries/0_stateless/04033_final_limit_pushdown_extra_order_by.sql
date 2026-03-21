-- Test that FINAL limit pushdown does NOT activate when ORDER BY extends
-- beyond or diverges from the sorting key prefix. Pushing limit into the
-- FINAL merge when the merge output order differs from the final ORDER BY
-- can produce incorrect results.
--
-- Part 1: Correctness with ReplacingMergeTree — ORDER BY (a, c) diverges
--         from key (a, b). Results must match with/without pushdown.
-- Part 2: EXPLAIN PIPELINE with AggregatingMergeTree — verify limit is
--         NOT pushed when ORDER BY extends beyond key, IS pushed when
--         ORDER BY matches key.

SET enable_analyzer = 1;
SET optimize_read_in_order = 1;
SET optimize_final_limit_pushdown = 1;
SET do_not_merge_across_partitions_select_final = 1;

-- ============================================================
-- Part 1: Correctness — ORDER BY diverges from sorting key
--
-- ReplacingMergeTree with key (a, b). Column c is NOT in the key.
-- After FINAL, rows are sorted by (a, b), but ORDER BY a, c sorts
-- differently. Without the fix, the limit would be incorrectly
-- pushed into the merge, potentially producing wrong top-N results.
-- ============================================================

DROP TABLE IF EXISTS t_extra_order;

CREATE TABLE t_extra_order
(
    a UInt32,
    b UInt32,
    c UInt32,
    ver UInt64
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY (a, b);

-- c is inversely related to b within each a-group.
-- After FINAL sorted by (a, b):
--   (1,1,30), (1,2,20), (1,3,10), (2,1,60), (2,2,50), ...
-- ORDER BY a, c ASC:
--   (1,3,10), (1,2,20), (1,1,30), (2,3,40), (2,2,50), ...
INSERT INTO t_extra_order VALUES
    (1, 1, 30, 1), (1, 2, 20, 1), (1, 3, 10, 1),
    (2, 1, 60, 1), (2, 2, 50, 1), (2, 3, 40, 1),
    (3, 1, 90, 1), (3, 2, 80, 1), (3, 3, 70, 1);

INSERT INTO t_extra_order VALUES
    (1, 1, 30, 2), (1, 2, 20, 2), (1, 3, 10, 2),
    (2, 1, 60, 2), (2, 2, 50, 2), (2, 3, 40, 2),
    (3, 1, 90, 2), (3, 2, 80, 2), (3, 3, 70, 2);

-- Both must produce identical results:
SELECT a, b, c
FROM t_extra_order FINAL
ORDER BY a, c ASC
LIMIT 5;

SELECT a, b, c
FROM t_extra_order FINAL
ORDER BY a, c ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

DROP TABLE t_extra_order;

-- ============================================================
-- Part 2: EXPLAIN PIPELINE with AggregatingMergeTree
--
-- AggregatingMergeTree supports limit pushdown (unlike Replacing).
-- Verify that the limit appears in EXPLAIN only when ORDER BY
-- matches the sorting key, not when it extends beyond.
-- ============================================================

DROP TABLE IF EXISTS t_extra_order_agg;

CREATE TABLE t_extra_order_agg
(
    a UInt32,
    b UInt32,
    c UInt32,
    v AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

INSERT INTO t_extra_order_agg
    SELECT number / 3 + 1, number % 3 + 1, number * 10,
           sumState(toUInt64(1))
    FROM numbers(9) GROUP BY number;

INSERT INTO t_extra_order_agg
    SELECT number / 3 + 1, number % 3 + 1, number * 10,
           sumState(toUInt64(100))
    FROM numbers(9) GROUP BY number;

-- ORDER BY (a, b, c) extends beyond key (a, b): limit must NOT be pushed
SELECT 'explain_no_pushdown_extra_col';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, b, c, finalizeAggregation(v) AS v
    FROM t_extra_order_agg FINAL
    ORDER BY a, b, c ASC
    LIMIT 5
)
WHERE explain LIKE '%Description: limit%';

-- ORDER BY (a, b) matches key exactly: limit SHOULD be pushed down
SELECT 'explain_pushdown_matching_key';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, b, finalizeAggregation(v) AS v
    FROM t_extra_order_agg FINAL
    ORDER BY a, b ASC
    LIMIT 5
)
WHERE explain LIKE '%Description: limit 5%';

DROP TABLE t_extra_order_agg;
