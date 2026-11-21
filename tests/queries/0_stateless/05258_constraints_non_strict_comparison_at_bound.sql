-- Constraint optimization must keep a `<=` / `>=` filter that the constraints allow only at equality.
-- https://github.com/ClickHouse/ClickHouse/issues/123168

DROP TABLE IF EXISTS t_constraint_bound;

CREATE TABLE t_constraint_bound
(
    a Int64,
    b Int64,
    CONSTRAINT c1 CHECK a <= 100,
    CONSTRAINT c2 CHECK a >= b
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_constraint_bound VALUES (100, 100), (100, 50), (50, 50), (40, 10);

SET convert_query_to_cnf = 1, optimize_using_constraints = 1;

SELECT a, b FROM t_constraint_bound WHERE a >= 100 ORDER BY a, b;
SELECT a, b FROM t_constraint_bound WHERE a <= b ORDER BY a, b;
SELECT a, b FROM t_constraint_bound PREWHERE a >= 100 ORDER BY a, b;
SELECT a, b FROM t_constraint_bound PREWHERE a <= b ORDER BY a, b;
SELECT a, count() FROM t_constraint_bound GROUP BY a HAVING a >= 100;
SELECT a, b FROM t_constraint_bound GROUP BY a, b HAVING a <= b ORDER BY a, b;

-- Swapped arguments, negated atoms, and an atom that is one of several in an OR group.
SELECT a, b FROM t_constraint_bound WHERE 100 <= a ORDER BY a, b;
SELECT a, b FROM t_constraint_bound WHERE b >= a ORDER BY a, b;
SELECT a, b FROM t_constraint_bound WHERE NOT (a < 100) ORDER BY a, b;
SELECT a, b FROM t_constraint_bound WHERE NOT (a > b) ORDER BY a, b;
SELECT a, b FROM t_constraint_bound WHERE a >= 100 OR b > 1000 ORDER BY a, b;

-- A constant of another type is not a node of the graph, so it is compared through the constant bounds of `a`.
SELECT a, b FROM t_constraint_bound WHERE a >= toInt64(100) ORDER BY a, b;

DROP TABLE t_constraint_bound;
