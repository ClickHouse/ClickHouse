-- The trivial `GROUP BY ... LIMIT` optimization must not fire when something between the
-- aggregation and the LIMIT consumes or filters the groups: cutting the aggregation at
-- `LIMIT + OFFSET` keys then changes the result instead of merely picking an unspecified
-- subset of the groups. Without these guards each of the cases below returned wrong
-- results (fewer rows or wrong values), as pinned by the expected outputs.
--
-- The top-level `INSERT ... SELECT` forms of these shapes (where
-- `OptimizeTrivialGroupByLimitPass` applies the settings-based rewrite) are covered by
-- test `04840_trivial_group_by_limit_projection_guards`. This test runs them as
-- subqueries, where the planner applies the kept-keys cutoff.

-- The guards live in the analyzer pass and the planner, and the `QUALIFY` query below
-- does not even parse into a plan with the old analyzer.
SET enable_analyzer = 1;

SET optimize_trivial_group_by_limit_query = 1;
SET max_threads = 16;
SET max_block_size = 100;

-- DISTINCT collapses the projected groups: 100000 groups yield 4 distinct values of
-- `intDiv(k, 25000)`, and LIMIT 3 must return 3 of them. With aggregation cut at 3 keys
-- the distinct set shrank to 1 value.
SELECT count() FROM (SELECT DISTINCT intDiv(k, 25000) AS d FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k LIMIT 3);

-- A window function in the projection is evaluated over all groups: `count() OVER ()`
-- must see all 100000 of them. With aggregation cut near the LIMIT it saw only the kept
-- groups (values like 100).
SELECT count(), min(w), max(w) FROM (SELECT count() OVER () AS w FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k LIMIT 5);

-- `arrayJoin` in the projection can drop rows (empty arrays): only 100 of the 100000
-- groups produce a row, so LIMIT 10 must still find 10 rows. With aggregation cut near
-- the LIMIT almost all kept groups produced nothing (1 row came out).
SELECT count() FROM (SELECT arrayJoin(if(k % 1000 = 0, [k], [])) AS a FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k LIMIT 10);

-- The `arrayJoin` guard matches by canonical function name: with `normalize_function_names = 0`
-- the case-insensitive alias `unnest` reaches the query tree unnormalized and must be
-- recognized all the same.
SELECT count() FROM (SELECT unnest(if(k % 1000 = 0, [k], [])) AS a FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k LIMIT 10) SETTINGS normalize_function_names = 0;

-- QUALIFY filters the groups after the aggregation: 1000 groups pass the filter, so
-- LIMIT 10 must find 10 rows, and the window function must see all 50000 groups of its
-- partition. With aggregation cut near the LIMIT none of the kept groups qualified
-- (0 rows came out). A QUALIFY on plain group keys alone is pushed down below the
-- aggregation and does not need the guard; the window function keeps it in place.
SELECT count(), min(w), max(w) FROM (SELECT count() OVER (PARTITION BY k % 2) AS w FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k QUALIFY k >= 99000 LIMIT 10);
