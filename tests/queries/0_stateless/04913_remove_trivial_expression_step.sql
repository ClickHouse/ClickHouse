-- https://github.com/ClickHouse/ClickHouse/issues/48749
-- A no-op Expression step (identity Before ORDER BY) should be removed from the plan.

SET query_plan_enable_optimizations = 1;
SET query_plan_merge_expressions = 1;

DROP TABLE IF EXISTS t_trivial_expr;
CREATE TABLE t_trivial_expr (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_trivial_expr VALUES (1, 'a'), (2, 'b');

SELECT * FROM t_trivial_expr ORDER BY id;

-- The identity "Before ORDER BY" Expression should be gone.
-- Match the step description, not a generic ORDER BY line (e.g. "Sorting for ORDER BY").
SELECT count() FROM
(
    EXPLAIN PLAN SELECT * FROM t_trivial_expr ORDER BY id
)
WHERE explain LIKE '%Before ORDER BY%';

-- A real Expression (id + 1) must stay. Check the function in the actions dump so
-- a leftover "Project names" Expression cannot satisfy this on its own.
SELECT count() > 0 FROM
(
    EXPLAIN PLAN actions = 1 SELECT id + 1 AS x FROM t_trivial_expr ORDER BY x
)
WHERE explain LIKE '%plus%';

DROP TABLE t_trivial_expr;
