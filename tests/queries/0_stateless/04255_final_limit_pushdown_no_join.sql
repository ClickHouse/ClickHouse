-- Regression: LIMIT must not be pushed below a JOIN into the FINAL merge.
-- Otherwise INNER JOIN can drop rows the merge already counted toward LIMIT,
-- producing a wrong top-N.

DROP TABLE IF EXISTS t_join_left;
DROP TABLE IF EXISTS t_join_right;

CREATE TABLE t_join_left
(
    key UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO t_join_left SELECT number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO t_join_left SELECT number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number;

CREATE TABLE t_join_right
(
    key UInt64,
    tag String
)
ENGINE = MergeTree
ORDER BY key;

-- Only odd keys are present in the right table: an INNER JOIN
-- must filter out every even key, including small ones near key = 0.
INSERT INTO t_join_right SELECT number, 'tag_' || toString(number) FROM numbers(100) WHERE number % 2 = 1;

SELECT 'inner_join_top5_pushdown_on';
SELECT l.key, r.tag, finalizeAggregation(l.val)
FROM t_join_left AS l FINAL
INNER JOIN t_join_right AS r ON l.key = r.key
ORDER BY l.key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1, query_plan_read_in_order_through_join = 1;

SELECT 'inner_join_top5_pushdown_off';
SELECT l.key, r.tag, finalizeAggregation(l.val)
FROM t_join_left AS l FINAL
INNER JOIN t_join_right AS r ON l.key = r.key
ORDER BY l.key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0, query_plan_read_in_order_through_join = 1;

-- Even with the optimization ON, no `Description: limit ...` must be attached
-- to the merging transform when a JOIN sits between the SortingStep and the
-- FINAL read.
SELECT 'explain_inner_join_no_limit_in_merge';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT l.key, r.tag, finalizeAggregation(l.val)
    FROM t_join_left AS l FINAL
    INNER JOIN t_join_right AS r ON l.key = r.key
    ORDER BY l.key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1, query_plan_read_in_order_through_join = 1
)
WHERE explain LIKE '%Description: limit%';

DROP TABLE t_join_left;
DROP TABLE t_join_right;
