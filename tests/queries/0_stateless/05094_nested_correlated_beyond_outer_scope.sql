-- A nested correlated subquery that references a column from a scope beyond its immediate outer query
-- is not supported. The planner says so where it buffers the outer stream; without a buffer the plan
-- reached the join's actions DAG and failed there with an internal `Cannot find column ... in actions
-- DAG input` instead.

SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_nested_correlated;
CREATE TABLE t_nested_correlated (ver UInt32, sp Int64) ENGINE = MergeTree ORDER BY ver;
INSERT INTO t_nested_correlated SELECT number % 5, number FROM numbers(20);

SELECT 'a reference that skips the intermediate scope';
SELECT count() FROM t_nested_correlated AS o WHERE EXISTS (
    SELECT 1 FROM t_nested_correlated AS i WHERE EXISTS (
        SELECT 1 FROM t_nested_correlated AS i2 WHERE i2.ver = o.ver)); -- { serverError NOT_IMPLEMENTED }

SELECT 'the same with the intermediate scope correlated as well';
SELECT count() FROM t_nested_correlated AS o WHERE EXISTS (
    SELECT 1 FROM t_nested_correlated AS i WHERE i.ver = o.ver AND EXISTS (
        SELECT 1 FROM t_nested_correlated AS i2 WHERE i2.ver = o.ver)); -- { serverError NOT_IMPLEMENTED }

SELECT 'each level referencing its own immediate outer query still works';
SELECT count() FROM t_nested_correlated AS o WHERE EXISTS (
    SELECT 1 FROM t_nested_correlated AS i WHERE i.ver = o.ver AND EXISTS (
        SELECT 1 FROM t_nested_correlated AS i2 WHERE i2.ver = i.ver));

SELECT 'and a single level of correlation works';
SELECT count() FROM t_nested_correlated AS o WHERE EXISTS (
    SELECT 1 FROM t_nested_correlated AS i WHERE i.ver = o.ver AND i.sp > 10);

DROP TABLE t_nested_correlated;
