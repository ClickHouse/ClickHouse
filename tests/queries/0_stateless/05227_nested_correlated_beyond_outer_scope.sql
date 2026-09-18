-- Some shapes of nested correlated subqueries are not supported yet, because the correlated column
-- is not available in the outer query plan where they are decorrelated. The planner says so where it
-- buffers the outer stream; without a buffer the plan reached the join's actions DAG and failed there
-- with an internal `Cannot find column ... in actions DAG input` instead.
--
-- The two layouts are decided by `correlated_subqueries_use_in_memory_buffer`, so every case runs
-- under both: the buffered one is rejected in `decorrelateQueryPlan`, the unbuffered one in
-- `buildLogicalJoin`. The join kind is pinned too, because the buffered layout forces `right`
-- and the unbuffered one otherwise follows the (randomizable) `compatibility` setting.

SET allow_experimental_correlated_subqueries = 1;
SET correlated_subqueries_default_join_kind = 'right';

DROP TABLE IF EXISTS t_nested_correlated;
CREATE TABLE t_nested_correlated (ver UInt32, sp Int64) ENGINE = MergeTree ORDER BY ver;
INSERT INTO t_nested_correlated SELECT number % 5, number FROM numbers(20);

SELECT '-- buffered layout';
SET correlated_subqueries_use_in_memory_buffer = 1;

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

SELECT 'a correlated reference through a derived table works';
SELECT count() FROM t_nested_correlated AS o WHERE EXISTS (
    SELECT 1 FROM (
        SELECT 1 FROM t_nested_correlated AS i2 WHERE i2.ver = o.ver));

SELECT 'and a single level of correlation works';
SELECT count() FROM t_nested_correlated AS o WHERE EXISTS (
    SELECT 1 FROM t_nested_correlated AS i WHERE i.ver = o.ver AND i.sp > 10);

SELECT '-- unbuffered layout';
SET correlated_subqueries_use_in_memory_buffer = 0;

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

SELECT 'a correlated reference through a derived table works';
SELECT count() FROM t_nested_correlated AS o WHERE EXISTS (
    SELECT 1 FROM (
        SELECT 1 FROM t_nested_correlated AS i2 WHERE i2.ver = o.ver));

SELECT 'and a single level of correlation works';
SELECT count() FROM t_nested_correlated AS o WHERE EXISTS (
    SELECT 1 FROM t_nested_correlated AS i WHERE i.ver = o.ver AND i.sp > 10);

DROP TABLE t_nested_correlated;
