SELECT arrayMap(x -> 1, [2]), 123 AS y;
SELECT arrayMap(x -> x + 1, [2]), 123 AS y;
SELECT arrayMap(x -> 1, [2, 3]), 123 AS y;
SELECT arrayMap(x -> x + 1, [2, 3]), 123 AS y;
