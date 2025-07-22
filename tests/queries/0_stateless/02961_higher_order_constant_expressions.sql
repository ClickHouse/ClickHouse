SET enable_analyzer = 1;

SELECT arrayMap(x -> x, [1, 2, 3]) AS x, isConstant(x);
SELECT arrayMap(x -> x + 1, [1, 2, 3]) AS x, isConstant(x);
SELECT arrayMap(x -> x + x, [1, 2, 3]) AS x, isConstant(x);
SELECT arrayMap((x, y) -> x + y, [1, 2, 3], [4, 5, 6]) AS x, isConstant(x);
SELECT arrayMap(x -> 1, [1, 2, 3]) AS x, isConstant(x);
SELECT arrayMap(x -> x + number, [1, 2, 3]) AS x, isConstant(x) FROM numbers(1);
SELECT arrayMap(x -> number, [1, 2, 3]) AS x, isConstant(x) FROM numbers(1);
SELECT arrayMax([1, 2, 3]) AS x, isConstant(x);

-- Does not work yet:
-- SELECT [1, 2, 3] IN arrayMap(x -> x, [1, 2, 3]);
