-- Tags: no-parallel
-- ^ User-defined SQL functions are stored at the server level (not per
-- database), so concurrent runs of this test would collide on the
-- `CREATE FUNCTION` global namespace and fail with `FUNCTION_ALREADY_EXISTS`.
-- All other UDF tests under `0_stateless/` use the same tag for this reason
-- (e.g. `01856_create_function`, `02098`-`02103`, `02125`).

-- Regression test for STID 4337-3161:
-- "Logical error: Incorrect ASTSelectWithUnionQuery (modes: 1, selects: 3)"
-- raised from `SelectIntersectExceptQueryMatcher::visit` when registering a
-- SQL UDF whose body contains a parenthesized inner UNION.
--
-- Root cause: `executeQueryImpl` runs `SelectIntersectExceptQueryVisitor` and
-- then `NormalizeSelectWithUnionQueryVisitor` on the parsed AST. The
-- normalizer flattens inner `UNION ALL` children into the parent's
-- `list_of_selects` and sets `is_normalized = true` (with `union_mode` =
-- `UNION_ALL`), but does NOT update `list_of_modes` to match. When the
-- `Interpreter` then runs `normalizeCreateFunctionQuery`, the
-- `SelectIntersectExceptQueryVisitor` runs a second time on the now-flattened
-- AST and triggers the assertion `list_of_modes.size() + 1 ==
-- list_of_selects->children.size()`.
--
-- The fix makes `SelectIntersectExceptQueryMatcher::visit` skip already
-- normalized union queries (no `INTERSECT` / `EXCEPT` modes can be present
-- after normalization, so there is nothing for the matcher to do).
--
-- See https://github.com/ClickHouse/ClickHouse/issues/103969

DROP FUNCTION IF EXISTS test_04214_repro;
DROP FUNCTION IF EXISTS test_04214_recursive;
DROP FUNCTION IF EXISTS test_04214_intersect;
DROP FUNCTION IF EXISTS test_04214_except;

-- Minimal reproducer: outer UNION ALL with a parenthesized inner UNION ALL.
CREATE FUNCTION test_04214_repro AS x ->
    (SELECT 1 UNION ALL (SELECT 1 UNION ALL SELECT 1));

SELECT 'minimal repro OK';

-- Full reproducer from issue #103969 (uses WITH RECURSIVE + parenthesized
-- UNION ALL inside the recursive CTE body).
CREATE FUNCTION test_04214_recursive AS x ->
    (SELECT DISTINCT number FROM numbers(2)
     UNION ALL
     WITH RECURSIVE r AS (SELECT 1 UNION ALL (SELECT * FROM numbers(2) UNION ALL SELECT 3))
     SELECT DISTINCT 1);

SELECT 'recursive repro OK';

-- INTERSECT inside a UDF body must still be normalized correctly (the fix
-- must not regress this path).
CREATE FUNCTION test_04214_intersect AS x -> (SELECT 1 INTERSECT SELECT 1);

SELECT 'intersect repro OK';

-- Same for EXCEPT.
CREATE FUNCTION test_04214_except AS x -> (SELECT 1 EXCEPT SELECT 1);

SELECT 'except repro OK';

DROP FUNCTION test_04214_repro;
DROP FUNCTION test_04214_recursive;
DROP FUNCTION test_04214_intersect;
DROP FUNCTION test_04214_except;
