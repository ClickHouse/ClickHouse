-- `arrayReduce` runs the aggregate its string argument names, so an unseeded `groupArraySample` makes
-- the expression non-deterministic. Reported as deterministic, its `HAVING` predicate was pushed below
-- the aggregation - evaluated once per row instead of once per group, so a group survived if any of its
-- rows drew favorably - and a constant-argument call was folded into a single draw for the query.

SELECT 'the predicate is evaluated per group';
-- Each of the 100 groups survives with probability 1/2 when the predicate is evaluated once per group,
-- and with probability 1 - 2^-100 when it is evaluated for each of the group's 100 rows.
SELECT count() < 100 FROM (SELECT number % 100 AS g FROM numbers(10000) GROUP BY g HAVING arrayReduce('groupArraySample(1)', [g, g + 1000])[1] < 1000);

SELECT 'a constant argument is not folded into one draw';
SELECT uniqExact(x) > 1 FROM (SELECT arrayReduce('groupArraySample(1)', [1, 2, 3])[1] AS x FROM numbers(200));

SELECT 'a seeded sample stays deterministic';
SELECT uniqExact(x) FROM (SELECT arrayReduce('groupArraySample(1, 42)', [1, 2, 3])[1] AS x FROM numbers(200));

SELECT 'a deterministic aggregate still folds';
SELECT arrayReduce('sum', [1, 2, 3]);
SELECT uniqExact(x) FROM (SELECT arrayReduce('max', [1, 2, 3]) AS x FROM numbers(200));
