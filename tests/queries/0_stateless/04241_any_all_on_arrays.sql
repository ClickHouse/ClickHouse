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

-- The lambda variable for the higher-order form must not collide with an
-- identifier on the LHS, so the parser walks the LHS and suffixes `_a` until
-- it finds a free name (`_a1`, `_a2`, ...).
SELECT _a < ANY([2]) FROM (SELECT 1 AS _a);
SELECT _a < ANY([0, 100]) FROM (SELECT 4 AS _a);
SELECT (_a + _a1) < ANY([100]) FROM (SELECT 1 AS _a, 2 AS _a1);
SELECT (_a + _a1 + _a2) < ANY([100]) FROM (SELECT 1 AS _a, 2 AS _a1, 3 AS _a2);

-- A table-qualified (compound) identifier such as `_a.x` binds its first part
-- (`_a`) during analysis, so the lambda variable must avoid that part too.
SELECT _a.x < ANY([2]) FROM (SELECT 1 AS x) AS _a;

-- Subquery form is still handled the old way (lowered to IN / NOT IN).
SELECT 3 = ANY(SELECT number FROM numbers(5));
SELECT 9 = ANY(SELECT number FROM numbers(5));
SELECT 3 <> ALL(SELECT number FROM numbers(5) WHERE number > 5);

-- NULL handling of the array form follows `has` (two-valued) for `=`/`<>` and
-- `arrayExists`/`arrayAll` (which fold unknown to `0`) for other operators.
-- This is rewritten at parse time, where settings are not available and a
-- per-row array column cannot use the analyzer's null-safe `IN` path, so it can
-- differ from the subquery form whose NULL handling depends on `transform_null_in`.
-- Documented here as the contract of the array form.
SELECT NULL = ANY([NULL]);   -- has([NULL], NULL)                  -> 1
SELECT NULL <> ALL([NULL]);  -- NOT has([NULL], NULL)              -> 0
SELECT NULL < ANY([1]);      -- arrayExists(_a -> NULL < _a, [1])  -> 0
SELECT NULL > ALL([1]);      -- arrayAll(_a -> NULL > _a, [1])     -> 0
