-- An `ON` condition that mentions only one side has no join key: the block nested loop join
-- evaluates it on the candidate pairs.

SELECT * FROM (SELECT NULL AS a, 1 AS b) AS foo
LEFT JOIN (SELECT 1024 AS b) AS bar
ON 1 = foo.b;

SELECT * FROM (SELECT NULL AS a, 1 AS b) AS foo
RIGHT JOIN (SELECT 1024 AS b) AS bar
ON 1 = bar.b;
