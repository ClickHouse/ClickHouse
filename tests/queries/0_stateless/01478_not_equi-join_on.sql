-- An `ON` condition that mentions only one side has no join key: the block nested loop join
-- evaluates it on the candidate pairs.
-- The swap of the join inputs would turn the LEFT join into a RIGHT one, which the operator does
-- not implement yet.
SET query_plan_join_swap_table = 'false';

SELECT * FROM (SELECT NULL AS a, 1 AS b) AS foo
LEFT JOIN (SELECT 1024 AS b) AS bar
ON 1 = foo.b;

SELECT * FROM (SELECT NULL AS a, 1 AS b) AS foo
RIGHT JOIN (SELECT 1024 AS b) AS bar
ON 1 = bar.b; -- { serverError NOT_IMPLEMENTED }
