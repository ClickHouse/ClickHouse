-- With `correlated_subqueries_use_in_memory_buffer = 0` a decorrelated subquery body is duplicated
-- by cloning it. A body carrying an `IN (subquery)` set used to be cloned together with the step
-- holding that set, and since a `FutureSetFromSubquery` source can be claimed only once, one of the
-- copies got no builder and its `in()` read a set that was never built:
--   Logical error: Not-ready Set is passed as the second argument for function 'in'
--
-- Every arm below asserts values that depend on the set's contents, so a set silently treated as
-- empty fails the arm instead of matching it. Each result is compared against the same query with
-- the buffer on, which does not duplicate the body.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

-- { echoOn }

-- Scalar correlated subquery over a body holding a partially matching IN set.
SELECT * FROM (SELECT (SELECT x) AS s, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 2) AS m GROUP BY x, m)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

SELECT * FROM (SELECT (SELECT x) AS s, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 2) AS m GROUP BY x, m)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

-- An empty set must read as empty, not as a set that happens to match.
SELECT * FROM (SELECT (SELECT x) AS s, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 9 WHERE 0) AS m GROUP BY x, m)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

SELECT * FROM (SELECT (SELECT x) AS s, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 9 WHERE 0) AS m GROUP BY x, m)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

-- A set matching every row, so a lost set flips every value rather than none.
SELECT * FROM (SELECT (SELECT x) AS s, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT arrayJoin([1, 2, 3])) AS m GROUP BY x, m)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

SELECT * FROM (SELECT (SELECT x) AS s, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT arrayJoin([1, 2, 3])) AS m GROUP BY x, m)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

-- Two sets in one body: all of them must be re-attached, not just the first.
SELECT * FROM (SELECT (SELECT x) AS s, m, n FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 2) AS m, x IN (SELECT 3) AS n GROUP BY x, m, n)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

SELECT * FROM (SELECT (SELECT x) AS s, m, n FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 2) AS m, x IN (SELECT 3) AS n GROUP BY x, m, n)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

-- EXISTS reaches the same decorrelation path as the scalar form.
SELECT x, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 2) AS m GROUP BY x, m) AS t WHERE EXISTS (SELECT 1 WHERE t.x > 1) ORDER BY x
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

SELECT x, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 2) AS m GROUP BY x, m) AS t WHERE EXISTS (SELECT 1 WHERE t.x > 1) ORDER BY x
SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

SELECT count() FROM (SELECT arrayJoin([1, 2, 3]) AS x GROUP BY x, x IN (SELECT 2)) AS t WHERE NOT EXISTS (SELECT 1 WHERE t.x > 2)
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

-- Both decorrelation join kinds duplicate the body, so neither arm may be relied on to build first.
SELECT * FROM (SELECT (SELECT x) AS s, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 2) AS m GROUP BY x, m)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_default_join_kind = 'left';

SELECT * FROM (SELECT (SELECT x) AS s, m FROM (SELECT arrayJoin([1, 2, 3]) AS x, x IN (SELECT 2) AS m GROUP BY x, m)) ORDER BY s
SETTINGS correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_default_join_kind = 'right';

-- Readiness is structural rather than a matter of scheduling: one set gate dominates the whole plan,
-- so a duplicated body cannot leave behind a second gate with no builder under it.
SELECT countIf(explain LIKE '%CreatingSet (Create set for subquery)%') AS builders, countIf(explain LIKE '%DelayedCreatingSets%') AS placeholders, countIf(explain LIKE '%CreatingSets (Create sets before main query execution)%') AS set_gates
FROM (EXPLAIN PLAN SELECT (SELECT x) FROM (SELECT arrayJoin([1, 2, 3]) AS x GROUP BY x, x IN (SELECT 2)) SETTINGS correlated_subqueries_use_in_memory_buffer = 0);

SELECT countIf(explain LIKE '%CreatingSet (Create set for subquery)%') AS builders, countIf(explain LIKE '%DelayedCreatingSets%') AS placeholders, countIf(explain LIKE '%CreatingSets (Create sets before main query execution)%') AS set_gates
FROM (EXPLAIN PLAN SELECT (SELECT x) FROM (SELECT arrayJoin([1, 2, 3]) AS x GROUP BY x, x IN (SELECT 2)) SETTINGS correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_default_join_kind = 'left');

-- { echoOff }
