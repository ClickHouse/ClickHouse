-- PostgreSQL-style `expr OP ANY(array)` / `expr OP ALL(array)` is recognised
-- at parse time and rewritten to `has`/`arrayExists`/`arrayAll` so it does not
-- require a query rewrite.

-- `=` and `<>` use the optimized `has` / `NOT has` path.
SELECT 3 = ANY([1, 2, 3, 4]);
SELECT 5 = ANY([1, 2, 3, 4]);
SELECT 5 = SOME([1, 2, 5]);
SELECT 3 <> ALL([1, 2, 3, 4]);
SELECT 9 <> ALL([1, 2, 3, 4]);
SELECT 3 != ALL([1, 2, 3, 4]);

-- Other comparison operators go through `arrayExists`/`arrayAll`.
SELECT 5 < ANY([1, 2, 6]);          -- exists x in arr where 5 < x  -> true
SELECT 5 < ANY([1, 2, 3]);          -- no element > 5             -> false
SELECT 5 > ALL([1, 2, 3]);          -- all < 5                    -> true
SELECT 5 > ALL([1, 2, 6]);          -- not all < 5                -> false
SELECT 5 >= ALL([1, 2, 5]);
SELECT 0 <= ALL([1, 2, 5]);

-- LHS expression is more than a bare identifier.
SELECT (number + 1) = ANY([2, 4]) FROM numbers(5) ORDER BY number;

-- Array expression can be a non-literal expression (column).
SELECT name = ANY(values)
FROM (SELECT 'b' AS name, ['a', 'b', 'c'] AS values);

-- Equivalence to the manual `has` / `arrayExists` rewrites.
SELECT (3 = ANY([1, 2, 3])) = has([1, 2, 3], 3);
SELECT (3 <> ALL([1, 2, 4])) = (NOT has([1, 2, 4], 3));
SELECT (5 < ANY([1, 2, 6])) = arrayExists(_a -> 5 < _a, [1, 2, 6]);
SELECT (5 > ALL([1, 2, 3])) = arrayAll(_a -> 5 > _a, [1, 2, 3]);

-- Subquery form is still handled the old way (lowered to IN / NOT IN).
SELECT 3 = ANY(SELECT number FROM numbers(5));
SELECT 9 = ANY(SELECT number FROM numbers(5));
SELECT 3 <> ALL(SELECT number FROM numbers(5) WHERE number > 5);
