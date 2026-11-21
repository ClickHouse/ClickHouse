-- A correlated subquery over a relation whose header carries a column name twice
-- (`SELECT number, *` yields two columns named `number`) is decorrelated into an
-- ANY RIGHT JOIN. With join runtime filters enabled the filter is built on that
-- duplicated key column, changing a downstream join input's column multiplicity and
-- aborting with `Block structure mismatch in JoinStep` in debug/sanitizer builds.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_join_runtime_filters = 1;
-- The runtime filter is only built for hash-family algorithms (supportsRuntimeFilter),
-- and CI randomizes join_algorithm. Pin it so the repro statements below deterministically
-- exercise the crashing runtime-filter path (they fail on the pre-fix build) instead of
-- silently passing under e.g. full_sorting_merge / partial_merge.
SET join_algorithm = 'hash';
-- The filter is only added for the RIGHT-build side (can_use_runtime_filter). CI randomizes
-- query_plan_optimize_join_order_randomize, which feeds random cardinalities to the join-order
-- optimizer and can swap ANY RIGHT JOIN into an ANY LEFT JOIN (build side becomes the left
-- input), for which no runtime filter is built. Pin query_plan_join_swap_table='false' so the
-- right table stays the build side and the filter is deterministically produced.
SET query_plan_join_swap_table = 'false';

-- { echoOn }
WITH t AS (SELECT number, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE t.number >= 0) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'right', correlated_subqueries_use_in_memory_buffer = 0;

WITH t AS (SELECT number, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE t.number >= 0) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'left', correlated_subqueries_use_in_memory_buffer = 0;

WITH t AS (SELECT number, *, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE t.number >= 0) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'right', correlated_subqueries_use_in_memory_buffer = 0;
-- { echoOff }

-- A runtime filter is still built for a normal join without duplicated column names.
-- Pin join_algorithm='hash': the filter is only added for hash-family algorithms
-- (supportsRuntimeFilter), and CI randomizes join_algorithm, so an unpinned run may
-- pick e.g. full_sorting_merge and build no filter, making this assertion flap.
SELECT countIf(explain LIKE '%BuildRuntimeFilter%') > 0
FROM (
    EXPLAIN PLAN
    SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
    ANY RIGHT JOIN (SELECT number AS b FROM numbers(3)) AS r ON l.a = r.b
    SETTINGS enable_join_runtime_filters = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false'
);

-- The filter must still be built when the build side has a duplicated NON-key column
-- (here the key `b` is unique and `c` is duplicated): only a duplicated KEY column breaks the
-- name-keyed filter machinery, so the guard is scoped to the join keys and must not disable the
-- filter for an unrelated duplicated column.
SELECT countIf(explain LIKE '%BuildRuntimeFilter%') > 0
FROM (
    EXPLAIN PLAN
    SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
    ANY RIGHT JOIN (SELECT number AS b, number + 1 AS c, c FROM numbers(3)) AS r ON l.a = r.b
    SETTINGS enable_join_runtime_filters = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false'
);

-- Post-`preCalculateKeys` shapes: when the join key is a computed expression, `preCalculateKeys`
-- appends it to the build header under its unqualified function name (e.g. `plus(__table3.b, 1_UInt8)`),
-- while a user projection of the same expression is qualified (`__table3.bp1`, or `__table3.`plus(b, 1)``
-- without an alias). The two names never collide, so the runtime filter is built on the single correct
-- column and results are correct (the guard checks the pre-`preCalculateKeys` build header, which is safe
-- because bailing out after `preCalculateKeys` would leave the join step's already-updated input header
-- desynced from its stream).
-- { echoOn }
SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
ANY RIGHT JOIN (SELECT number AS b, b + 1 AS bp1 FROM numbers(3)) AS r ON l.a = r.b + 1
ORDER BY 1;

SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
ANY RIGHT JOIN (SELECT *, b + 1 FROM (SELECT number AS b FROM numbers(3))) AS r ON l.a = r.b + 1
ORDER BY 1;

-- The one post-`preCalculateKeys` duplicate that IS reachable: two predicates sharing the same computed
-- key append `plus(__table3.b, 1_UInt8)` to the build header twice. Unlike the plain-column crash at the
-- top of this test, this duplicate is a freshly-computed key that every build-subtree step declares
-- consistently, so there is no downstream `Block structure mismatch`; the two filters are redundant (both
-- address the first column), not wrong. Verified for `hash` and `parallel_hash`.
SELECT * FROM (SELECT number AS a, number AS a2 FROM numbers(100)) AS l
ANY RIGHT JOIN (SELECT number AS b FROM numbers(3)) AS r ON l.a = r.b + 1 AND l.a2 = r.b + 1
ORDER BY 1;

SELECT * FROM (SELECT number AS a, number AS a2 FROM numbers(100)) AS l
ANY RIGHT JOIN (SELECT number AS b FROM numbers(3)) AS r ON l.a = r.b + 1 AND l.a2 = r.b + 1
ORDER BY 1
SETTINGS join_algorithm = 'parallel_hash';
-- { echoOff }

-- The runtime filter must still be built (not over-restricted) when the build side projects a copy of
-- the computed join key: the qualified projection and the unqualified appended key do not collide.
SELECT countIf(explain LIKE '%BuildRuntimeFilter%') > 0
FROM (
    EXPLAIN PLAN
    SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
    ANY RIGHT JOIN (SELECT number AS b, b + 1 AS bp1 FROM numbers(3)) AS r ON l.a = r.b + 1
    SETTINGS enable_join_runtime_filters = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false'
);

-- Mixed predicates: the duplicate-key guard collects only the build column of a genuine left/right
-- equi-join pair, not the build column of a single-side local filter. Here the join key is `b` (unique)
-- and `c` is a duplicated NON-key column guarded by `r.c = 1`; the filter is only ever built on `b`, so
-- the duplicated `c` must not disable it.
SELECT countIf(explain LIKE '%BuildRuntimeFilter%') > 0
FROM (
    EXPLAIN PLAN
    SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
    ANY RIGHT JOIN (SELECT number AS b, 1 AS c, c FROM numbers(3)) AS r ON l.a = r.b AND r.c = 1
    SETTINGS enable_join_runtime_filters = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false'
);
