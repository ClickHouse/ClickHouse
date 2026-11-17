-- Pins the merging of duplicate MATERIALIZED CTEs (same CTE name + same subquery)
-- across UNION ALL branches: such CTEs must be materialized exactly once and shared
-- by all their usages, while CTEs that are genuinely single-use still get inlined.
--
-- Temporary table names created for a materialized CTE are random, so raw `EXPLAIN`
-- output is never pinned directly for the merged cases; instead we count how many
-- `MaterializingCTE (Materializing CTE: <name>)` plan-step lines the query plan has.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- 1. Two-branch UNION ALL, same CTE, referenced once per branch: single materialization.
SELECT count() FROM (
    EXPLAIN
    WITH t AS MATERIALIZED (SELECT number FROM numbers(10))
    SELECT number FROM t UNION ALL SELECT number FROM t
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE:%';

-- 1b. Functional pin using nondeterminism: a single materialization means both
--     UNION branches observe the same random value, so uniqExact(r) is 1.
SELECT uniqExact(r) FROM (
    WITH t AS MATERIALIZED (SELECT rand() AS r)
    SELECT r FROM t UNION ALL SELECT r FROM t
);

-- 2. Three-branch UNION ALL, same CTE: still a single materialization.
SELECT count() FROM (
    EXPLAIN
    WITH t AS MATERIALIZED (SELECT number FROM numbers(10))
    SELECT number FROM t UNION ALL SELECT number FROM t UNION ALL SELECT number FROM t
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE:%';

-- 3. Two differently-named materialized CTEs with the identical subquery body, each
--    used twice across a 4-way UNION: the merge key is (cte name, subquery), so
--    different names must NOT merge with each other -- two materializations, one per name.
SELECT count() FROM (
    EXPLAIN
    WITH a AS MATERIALIZED (SELECT number FROM numbers(10)), b AS MATERIALIZED (SELECT number FROM numbers(10))
    SELECT number FROM a UNION ALL SELECT number FROM a UNION ALL SELECT number FROM b UNION ALL SELECT number FROM b
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE:%';

-- 4. Same CTE name, different body, in sibling scopes (each parenthesized subquery
--    defines its own `t` and references it twice): both `t`s must materialize
--    independently -- two materializations, not one.
SELECT count() FROM (
    EXPLAIN
    (WITH t AS MATERIALIZED (SELECT number FROM numbers(3)) SELECT number FROM t UNION ALL SELECT number FROM t)
    UNION ALL
    (WITH t AS MATERIALIZED (SELECT number + 100 AS number FROM numbers(3)) SELECT number FROM t UNION ALL SELECT number FROM t)
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE:%';

-- 4b. Data assertion for the same query (ORDER BY for stable output): each sibling
--     scope's materialized CTE contributes its own 3 values, doubled by its own UNION.
SELECT number FROM (
    (WITH t AS MATERIALIZED (SELECT number FROM numbers(3)) SELECT number FROM t UNION ALL SELECT number FROM t)
    UNION ALL
    (WITH t AS MATERIALIZED (SELECT number + 100 AS number FROM numbers(3)) SELECT number FROM t UNION ALL SELECT number FROM t)
) ORDER BY number;

-- 5. Nested materialized CTE under UNION: `outer_cte` references `inner_cte`, and
--    `outer_cte` itself is referenced from both UNION branches. Merging is post-order
--    (inner CTE merged/materialized before the outer one), so there are two logical
--    materializations here: one for `inner_cte`, one for `outer_cte`.
SELECT count() FROM (
    EXPLAIN
    WITH inner_cte AS MATERIALIZED (SELECT number FROM numbers(3)),
         outer_cte AS MATERIALIZED (SELECT number * 2 AS number FROM inner_cte)
    SELECT number FROM outer_cte UNION ALL SELECT number FROM outer_cte
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE:%';

-- 5b. Data assertion for the same query (ORDER BY for stable output).
SELECT number FROM (
    WITH inner_cte AS MATERIALIZED (SELECT number FROM numbers(3)),
         outer_cte AS MATERIALIZED (SELECT number * 2 AS number FROM inner_cte)
    SELECT number FROM outer_cte UNION ALL SELECT number FROM outer_cte
) ORDER BY number;

-- 6. Single-use materialized CTE (no UNION, no other reference): still inlined,
--    exactly as without this task's merging logic -- zero materializations.
SELECT count() FROM (
    EXPLAIN
    WITH t AS MATERIALIZED (SELECT number FROM numbers(10))
    SELECT number FROM t
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE:%';

-- 7. UNION ALL branch plus an IN-subquery branch referencing the same CTE: all three
--    usages (the two in the first branch, one direct and one inside IN, plus the
--    second UNION branch) share a single materialization.
SELECT count() FROM (
    EXPLAIN
    WITH t AS MATERIALIZED (SELECT number FROM numbers(3))
    SELECT number FROM t WHERE number IN (SELECT number FROM t) UNION ALL SELECT number FROM t
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE:%';

-- 7b. Data assertion for the same query (ORDER BY for stable output).
SELECT number FROM (
    WITH t AS MATERIALIZED (SELECT number FROM numbers(3))
    SELECT number FROM t WHERE number IN (SELECT number FROM t) UNION ALL SELECT number FROM t
) ORDER BY number;

-- 8. Two sibling scopes, each independently defining `t AS MATERIALIZED (...)` with the
--    SAME name and a structurally identical body, each `t` referenced twice, the two
--    scopes combined by an outer UNION ALL. The merge key (CTE name + subquery hash) has
--    no scope component, so the two independent definitions merge into a single
--    materialization -- this is intended (and unavoidable, see the design doc), not a bug.
SELECT count() FROM (
    EXPLAIN
    (WITH t AS MATERIALIZED (SELECT number FROM numbers(3)) SELECT number FROM t UNION ALL SELECT number FROM t)
    UNION ALL
    (WITH t AS MATERIALIZED (SELECT number FROM numbers(3)) SELECT number FROM t UNION ALL SELECT number FROM t)
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE:%';

-- 8b. Functional pin using nondeterminism: a single shared materialization across both
--     sibling scopes means all four references observe the same random value.
SELECT uniqExact(r) FROM (
    (WITH t AS MATERIALIZED (SELECT rand() AS r) SELECT r FROM t UNION ALL SELECT r FROM t)
    UNION ALL
    (WITH t AS MATERIALIZED (SELECT rand() AS r) SELECT r FROM t UNION ALL SELECT r FROM t)
);
