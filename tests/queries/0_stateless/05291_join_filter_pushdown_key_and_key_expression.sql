-- A filter pushed through a JOIN on both a column and an expression of that column, e.g.
-- `t1.k = t2.k AND intDiv(t1.k, 2) = intDiv(t2.k, 2)`, keeps its meaning on the other side.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t1 SELECT number, number FROM numbers(20000);
INSERT INTO t2 SELECT number, number FROM numbers(20000);

-- WHERE on the key expression of one side
SELECT count() FROM t1 JOIN t2 ON t1.k = t2.k AND intDiv(t1.k, 2) = intDiv(t2.k, 2)
WHERE intDiv(t2.k, 2) >= 100
SETTINGS enable_join_runtime_filters = 0, query_plan_filter_push_down = 1;

-- The same WHERE, pushed down only after the JOIN runtime filters are added
SELECT count() FROM t1 JOIN t2 ON t1.k = t2.k AND intDiv(t1.k, 2) = intDiv(t2.k, 2)
WHERE intDiv(t2.k, 2) >= 100
SETTINGS enable_join_runtime_filters = 1, query_plan_filter_push_down = 0;

-- A non-equi ON condition over both key expressions
SELECT count() FROM t1 JOIN t2 ON t1.k = t2.k AND intDiv(t1.k, 2) = intDiv(t2.k, 2) AND intDiv(t2.k, 2) >= intDiv(t1.k, 2)
SETTINGS enable_join_runtime_filters = 1;

-- The same condition in WHERE
SELECT count() FROM t1 JOIN t2 ON t1.k = t2.k AND intDiv(t1.k, 2) = intDiv(t2.k, 2)
WHERE intDiv(t2.k, 2) >= intDiv(t1.k, 2)
SETTINGS enable_join_runtime_filters = 0, query_plan_filter_push_down = 1;

-- LEFT JOIN: the copy pushed to the right side must not lose matches
SELECT count(), countIf(t1.k = t2.k) FROM t1 LEFT JOIN t2 ON t1.k = t2.k AND intDiv(t1.k, 2) = intDiv(t2.k, 2)
WHERE intDiv(t1.k, 2) >= 100
SETTINGS enable_join_runtime_filters = 0, query_plan_filter_push_down = 1;

-- The WHERE is still pushed down to both sides, as the same expression
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t1 JOIN t2 ON t1.k = t2.k AND intDiv(t1.k, 2) = intDiv(t2.k, 2)
    WHERE intDiv(t2.k, 2) >= 100
    SETTINGS enable_join_runtime_filters = 0, query_plan_filter_push_down = 1,
             optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, enable_parallel_replicas = 0
) WHERE explain ILIKE '%Filter column: k DIV 2 >= 100%';

-- A key expression of another type (issue #122613)
SELECT count() FROM t1 JOIN t2 ON t1.k = t2.k AND CAST(t1.k AS Decimal(38, 6)) = CAST(t2.k AS Decimal(38, 6))
    AND CAST(t2.k AS Decimal(38, 6)) >= CAST(t1.k AS Decimal(38, 6))
SETTINGS enable_join_runtime_filters = 1;

-- Keys on a column and on two nested expressions of it
SELECT count() FROM t1 JOIN t2 ON t1.k = t2.k AND intDiv(t1.k, 2) = intDiv(t2.k, 2)
    AND intDiv(intDiv(t1.k, 2), 2) = intDiv(intDiv(t2.k, 2), 2)
WHERE intDiv(intDiv(t2.k, 2), 2) >= 50
SETTINGS enable_join_runtime_filters = 0, query_plan_filter_push_down = 1;

DROP TABLE t1;
DROP TABLE t2;
