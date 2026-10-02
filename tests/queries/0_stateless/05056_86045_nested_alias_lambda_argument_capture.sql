-- A lambda argument that an enclosing alias resolution already hid is not visible to a nested
-- aliased expression either, so it must not keep the argument of an inner lambda visible.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

-- While `a` is resolved, the argument `x` of the outer lambda is hidden, so `x` in `b` is the
-- column of the query (5), not the argument of the lambda that uses `b`: `[[7, 8]]`.
WITH (WITH x + 1 AS b SELECT arrayMap(x -> x + b, [1, 2])) AS a
SELECT arrayMap(x -> a, [10]) FROM (SELECT 5 AS x);

-- The same shape without a subquery in between.
WITH x + 1 AS b, arrayMap(x -> x + b, [1, 2]) AS a
SELECT arrayMap(x -> a, [10]) FROM (SELECT 5 AS x);

-- Nothing outside of the lambdas provides `y`, so the innermost argument stays visible.
WITH (WITH y + 1 AS b SELECT arrayMap(y -> y + b, [1, 2])) AS a
SELECT arrayMap(y -> a, [10]);
