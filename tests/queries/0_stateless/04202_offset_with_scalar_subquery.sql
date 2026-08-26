-- Test: scalar subquery inside OFFSET / LIMIT BY OFFSET / LIMIT BY LIMIT expression
-- Covers: src/Analyzer/Resolve/resolveFunction.cpp:1013-1024 — the `__getScalar`
-- constant-folding branch. The PR's own test exercises this only via the LIMIT clause
-- (`LIMIT (select sum(number), count() from numbers(3)).1`). OFFSET, LIMIT BY LIMIT, and
-- LIMIT BY OFFSET each call `convertLimitOffsetExpression` separately and would silently
-- regress to `INVALID_LIMIT_EXPRESSION: ... expression must be constant` if the
-- `__getScalar` fold path is ever broken for those clauses specifically.
--
-- The covered code path lives in the new analyzer only; under the old analyzer
-- scalar subqueries in these clauses still raise `INVALID_LIMIT_EXPRESSION`. Pin
-- the new analyzer so the test runs cleanly under both stateless suites.

SET enable_analyzer = 1;

-- { echo }
SELECT number FROM numbers(10) ORDER BY number LIMIT 5 OFFSET 1 + (SELECT count() FROM numbers(2));
SELECT number FROM numbers(10) ORDER BY number LIMIT 5 OFFSET (SELECT sum(number), count() FROM numbers(3)).2;
SELECT number FROM numbers(10) ORDER BY number OFFSET (SELECT sum(number), count() FROM numbers(3)).2;
SELECT number % 3 AS k FROM numbers(10) ORDER BY number, k LIMIT 1 + (SELECT count() FROM numbers(1)) BY k;
SELECT number % 3 AS k FROM numbers(20) ORDER BY number, k LIMIT 1 + (SELECT count() FROM numbers(1)) OFFSET (SELECT count() FROM numbers(3)) BY k;
