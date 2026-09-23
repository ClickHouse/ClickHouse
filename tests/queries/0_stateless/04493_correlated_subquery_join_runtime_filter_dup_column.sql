-- A correlated subquery over a relation whose header carries a column name twice
-- (`SELECT number, *` yields two columns named `number`) is decorrelated into an
-- ANY RIGHT JOIN. With join runtime filters enabled the filter is built on that
-- duplicated key column, changing a downstream join input's column multiplicity and
-- aborting with `Block structure mismatch in JoinStep` in debug/sanitizer builds.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
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

-- preCalculateKeys updates the logical join before runtime-filter planning rejects the duplicated
-- build key. The required expression re-merge must not also override disabled filter rewrites.
SELECT '-- failed runtime filter attempt honors disabled filter rewrites';
SELECT countIf(explain LIKE '%Filter (%') = 2 AND countIf(explain LIKE 'Filter (%') = 1
FROM (
    EXPLAIN PLAN actions = 1
    SELECT *
    FROM
    (
        SELECT *
        FROM
        (
            SELECT l.a, r.number
            FROM (SELECT number AS a FROM numbers(100)) AS l
            ANY RIGHT JOIN (SELECT number, * FROM numbers(3)) AS r ON l.a = r.number
            ORDER BY a
        )
        WHERE a > 10
    )
    WHERE a < 90
    SETTINGS query_plan_filter_push_down = 0, query_plan_merge_filters = 0,
             query_plan_merge_expressions = 0, enable_parallel_replicas = 0
);

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
-- (here the key `b` has a unique name-and-type position and `c` does not): resolving the actual
-- key position must not disable the filter for an unrelated duplicated column.
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
-- without an alias). The two names never collide, so the runtime filter resolves the single correct
-- name-and-type position and results are correct.
-- { echoOn }
SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
ANY RIGHT JOIN (SELECT number AS b, b + 1 AS bp1 FROM numbers(3)) AS r ON l.a = r.b + 1
ORDER BY 1;

SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
ANY RIGHT JOIN (SELECT *, b + 1 FROM (SELECT number AS b FROM numbers(3))) AS r ON l.a = r.b + 1
ORDER BY 1;

-- When two predicates share the same computed key, `preCalculateKeys` keeps both predicate keys but
-- exposes their shared calculation in the build header only once. Both predicates can therefore resolve
-- the same unique position and retain their two redundant filters. Verify correctness with both `hash`
-- and `parallel_hash`.
SELECT * FROM (SELECT number AS a, number AS a2 FROM numbers(100)) AS l
ANY RIGHT JOIN (SELECT number AS b FROM numbers(3)) AS r ON l.a = r.b + 1 AND l.a2 = r.b + 1
ORDER BY 1;

SELECT * FROM (SELECT number AS a, number AS a2 FROM numbers(100)) AS l
ANY RIGHT JOIN (SELECT number AS b FROM numbers(3)) AS r ON l.a = r.b + 1 AND l.a2 = r.b + 1
ORDER BY 1
SETTINGS join_algorithm = 'parallel_hash';
-- { echoOff }

SELECT countIf(explain LIKE '%BuildRuntimeFilter%') = 2
FROM (
    EXPLAIN PLAN
    SELECT * FROM (SELECT number AS a, number AS a2 FROM numbers(100)) AS l
    ANY RIGHT JOIN (SELECT number AS b FROM numbers(3)) AS r ON l.a = r.b + 1 AND l.a2 = r.b + 1
    SETTINGS enable_join_runtime_filters = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false'
);

-- The runtime filter must still be built (not over-restricted) when the build side projects a copy of
-- the computed join key: the qualified projection and the unqualified appended key do not collide.
SELECT countIf(explain LIKE '%BuildRuntimeFilter%') > 0
FROM (
    EXPLAIN PLAN
    SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
    ANY RIGHT JOIN (SELECT number AS b, b + 1 AS bp1 FROM numbers(3)) AS r ON l.a = r.b + 1
    SETTINGS enable_join_runtime_filters = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false'
);

-- Mixed predicates: runtime-filter keys contain only genuine left/right equi-join pairs, not the build
-- column of a single-side local filter. Here the join key is `b` (unique) and `c` is a duplicated NON-key
-- column guarded by `r.c = 1`; the duplicated `c` must not disable the filter built on `b`.
SELECT countIf(explain LIKE '%BuildRuntimeFilter%') > 0
FROM (
    EXPLAIN PLAN
    SELECT * FROM (SELECT number AS a FROM numbers(100)) AS l
    ANY RIGHT JOIN (SELECT number AS b, 1 AS c, c FROM numbers(3)) AS r ON l.a = r.b AND r.c = 1
    SETTINGS enable_join_runtime_filters = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false'
);

-- A multi-key LEFT ANTI JOIN uses one tuple filter. If any build key is ambiguous, the whole tuple
-- filter must fail closed: per-column NOT IN filters would be incorrect for a composite key.
SELECT countIf(explain LIKE '%BuildRuntimeFilter%') = 0
FROM (
    EXPLAIN PLAN
    SELECT l.a, l.a2
    FROM (SELECT number AS a, number + 10 AS a2 FROM numbers(5)) AS l
    LEFT ANTI JOIN (SELECT number, *, number + 10 AS c FROM numbers(3)) AS r
        ON l.a = r.number AND l.a2 = r.c
    SETTINGS enable_join_runtime_filters = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false'
);

-- { echoOn }
SELECT l.a, l.a2
FROM (SELECT number AS a, number + 10 AS a2 FROM numbers(5)) AS l
LEFT ANTI JOIN (SELECT number, *, number + 10 AS c FROM numbers(3)) AS r
    ON l.a = r.number AND l.a2 = r.c
ORDER BY l.a;
-- { echoOff }
