-- PostgreSQL-style `expr OP SOME(array)` / `expr OP ALL(array)` is recognised at
-- parse time and rewritten to `has` / `arrayExists` / `arrayAll`, so it does not
-- require a manual query rewrite.
--
-- `ANY` is intentionally not accepted for the array form: `any` is also an
-- aggregate function, so `expr OP any(x)` keeps its function-call meaning. Use
-- `SOME` (the SQL synonym for the existential quantifier) instead.

-- `=` and `<>` use the optimized `has` / `NOT has` path.
SELECT 3 = SOME([1, 2, 3, 4]);
SELECT 5 = SOME([1, 2, 3, 4]);
SELECT 5 = SOME([1, 2, 5]);
SELECT 3 <> ALL([1, 2, 3, 4]);
SELECT 9 <> ALL([1, 2, 3, 4]);
SELECT 3 != ALL([1, 2, 3, 4]);

-- Other comparison operators go through `arrayExists`/`arrayAll`.
SELECT 5 < SOME([1, 2, 6]);         -- exists x in arr where 5 < x  -> true
SELECT 5 < SOME([1, 2, 3]);         -- no element > 5              -> false
SELECT 5 > ALL([1, 2, 3]);          -- all < 5                     -> true
SELECT 5 > ALL([1, 2, 6]);          -- not all < 5                 -> false
SELECT 5 >= ALL([1, 2, 5]);
SELECT 0 <= ALL([1, 2, 5]);

-- LHS expression is more than a bare identifier.
SELECT (number + 1) = SOME([2, 4]) FROM numbers(5) ORDER BY number;

-- Array expression can be a non-literal expression (column).
SELECT name = SOME(values)
FROM (SELECT 'b' AS name, ['a', 'b', 'c'] AS values);

-- Equivalence to the manual `has` / `arrayExists` rewrites.
SELECT (3 = SOME([1, 2, 3])) = has([1, 2, 3], 3);
SELECT (3 <> ALL([1, 2, 4])) = (NOT has([1, 2, 4], 3));
SELECT (5 < SOME([1, 2, 6])) = arrayExists(_a -> 5 < _a, [1, 2, 6]);
SELECT (5 > ALL([1, 2, 3])) = arrayAll(_a -> 5 > _a, [1, 2, 3]);

-- Besides symbolic comparison operators, the array form also accepts the keyword
-- comparison predicates `IS DISTINCT FROM` and `IS NOT DISTINCT FROM`, routed through
-- the `arrayExists` / `arrayAll` form.
SELECT 1 IS DISTINCT FROM ALL([2, 3]);      -- distinct from each          -> 1
SELECT 1 IS DISTINCT FROM ALL([1, 2]);      -- not distinct from 1         -> 0
SELECT 1 IS DISTINCT FROM SOME([1, 2]);     -- distinct from 2             -> 1
SELECT 1 IS NOT DISTINCT FROM SOME([1, 2]); -- not distinct from 1         -> 1
SELECT 1 IS NOT DISTINCT FROM ALL([1, 2]);  -- distinct from 2             -> 0
SELECT (1 IS DISTINCT FROM ALL([2, 3])) = arrayAll(_a -> 1 IS DISTINCT FROM _a, [2, 3]);

-- The array form also accepts the string-search predicates `LIKE`, `ILIKE`, `NOT LIKE`,
-- `NOT ILIKE`, and `REGEXP`, routed through the same `arrayExists` / `arrayAll` rewrite.
-- `MatchImpl` supports a constant haystack with a non-constant needle, so the lambda
-- variable can be the pattern (needle) argument without throwing `ILLEGAL_COLUMN`.
SELECT 'abc' LIKE SOME(['a%', 'b%']);       -- exists: 'abc' LIKE 'a%'              -> 1
SELECT 'abc' LIKE ALL(['a%', 'b%']);        -- not all: 'abc' NOT LIKE 'b%'         -> 0
SELECT 'abc' ILIKE SOME(['A%']);            -- case-insensitive 'abc' LIKE 'A%'     -> 1
SELECT 'abc' NOT LIKE SOME(['x%', 'a%']);   -- exists: 'abc' NOT LIKE 'x%'          -> 1
SELECT 'abc' NOT LIKE ALL(['x%', 'y%']);    -- all: matches neither pattern         -> 1
SELECT 'abc' NOT ILIKE SOME(['X%']);        -- exists: 'abc' NOT ILIKE 'X%'         -> 1
SELECT 'abc' REGEXP SOME(['^a', 'z']);      -- exists: 'abc' matches '^a'           -> 1
SELECT 'abc' REGEXP ALL(['^a', 'z']);       -- not all: 'abc' does not match 'z'    -> 0
-- Equivalence to the explicit lambda form for the string-search predicates.
SELECT ('abc' LIKE SOME(['a%', 'b%'])) = arrayExists(_a -> 'abc' LIKE _a, ['a%', 'b%']);
SELECT ('abc' NOT LIKE ALL(['x%', 'y%'])) = arrayAll(_a -> 'abc' NOT LIKE _a, ['x%', 'y%']);
SELECT ('abc' REGEXP SOME(['^a', 'z'])) = arrayExists(_a -> match('abc', _a), ['^a', 'z']);
-- A `NULL` pattern folds to `0` (unknown) like the other higher-order forms above.
SELECT 'abc' LIKE SOME(['a%', NULL]);       -- exists: 'abc' LIKE 'a%' (NULL -> 0)  -> 1
SELECT 'abc' LIKE ALL(['a%', NULL]);        -- all: 'abc' LIKE NULL folds to 0      -> 0

-- The lambda variable for the higher-order form must not collide with an
-- identifier on the LHS, so the parser walks the LHS and suffixes `_a` until
-- it finds a free name (`_a1`, `_a2`, ...).
SELECT _a < SOME([2]) FROM (SELECT 1 AS _a);
SELECT _a < SOME([0, 100]) FROM (SELECT 4 AS _a);
SELECT (_a + _a1) < SOME([100]) FROM (SELECT 1 AS _a, 2 AS _a1);
SELECT (_a + _a1 + _a2) < SOME([100]) FROM (SELECT 1 AS _a, 2 AS _a1, 3 AS _a2);

-- A table-qualified (compound) identifier such as `_a.x` binds its first part
-- (`_a`) during analysis, so the lambda variable must avoid that part too.
SELECT _a.x < SOME([2]) FROM (SELECT 1 AS x) AS _a;

-- Subquery form is still handled the old way (lowered to IN / NOT IN), for
-- `ANY` / `SOME` / `ALL` alike.
SELECT 3 = ANY(SELECT number FROM numbers(5));
SELECT 3 = SOME(SELECT number FROM numbers(5));
SELECT 9 = ANY(SELECT number FROM numbers(5));
SELECT 3 <> ALL(SELECT number FROM numbers(5) WHERE number > 5);

-- `ANY` is intentionally not an array quantifier (only `SOME` / `ALL` are).
-- Written as a function call, `any(...)` keeps its aggregate-function meaning, so
-- `1 = any([1, 2, 3])` compares `1` against the aggregated array value and fails
-- the type check rather than becoming `has([1, 2, 3], 1)`.
SELECT 1 = any([1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- NULL handling of the array form follows `has` (two-valued) for `=`/`<>` and
-- `arrayExists`/`arrayAll` (which fold an unknown result to `0`) for other
-- operators. This is rewritten at parse time, where settings are not available
-- and a per-row array column cannot use the analyzer's null-safe `IN` path, so
-- it can differ from the subquery form whose NULL handling depends on
-- `transform_null_in`. Documented here as the contract of the array form.
SELECT NULL = SOME([NULL]);   -- has([NULL], NULL)                  -> 1
SELECT NULL <> ALL([NULL]);   -- NOT has([NULL], NULL)              -> 0
SELECT NULL < SOME([1]);      -- arrayExists(_a -> NULL < _a, [1])  -> 0
SELECT NULL > ALL([1]);       -- arrayAll(_a -> NULL > _a, [1])     -> 0

-- The keyword predicates `IS DISTINCT FROM` / `IS NOT DISTINCT FROM` carry their own
-- null-safe semantics through the same `arrayExists` / `arrayAll` rewrite: they treat
-- `NULL` as an ordinary comparable value and always return `0`/`1` (never `NULL`), so the
-- result must not fold to `0` the way the `<` / `>` forms above do. These cases pin that
-- behaviour and would give a different answer if the rewrite were `equals` / `notEquals`.
SELECT NULL IS DISTINCT FROM SOME([NULL, 1]);     -- exists: NULL distinct from 1               -> 1
SELECT NULL IS DISTINCT FROM ALL([NULL, 1]);      -- all: NULL not distinct from NULL           -> 0
SELECT NULL IS NOT DISTINCT FROM SOME([NULL, 1]); -- exists: NULL not distinct from NULL (= 0)  -> 1
SELECT NULL IS NOT DISTINCT FROM ALL([NULL, 1]);  -- all: NULL distinct from 1                  -> 0
SELECT 1 IS DISTINCT FROM SOME([NULL]);           -- exists: 1 distinct from NULL (!= 0)        -> 1
SELECT 1 IS DISTINCT FROM ALL([NULL]);            -- all: 1 distinct from NULL (!= 0)           -> 1
SELECT 1 IS NOT DISTINCT FROM SOME([NULL, 1]);    -- exists: 1 not distinct from 1              -> 1
-- The keyword form must stay equivalent to the explicit lambda form even with `NULL`.
SELECT (NULL IS NOT DISTINCT FROM SOME([NULL, 1])) = arrayExists(_a -> NULL IS NOT DISTINCT FROM _a, [NULL, 1]);
SELECT (1 IS DISTINCT FROM ALL([NULL])) = arrayAll(_a -> 1 IS DISTINCT FROM _a, [NULL]);
