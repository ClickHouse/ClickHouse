-- Pins that a non-recursive helper CTE with a plain `SELECT` body declared `MATERIALIZED`
-- inside a `WITH RECURSIVE` clause is materialized once, and that every recursive iteration
-- reads the same temporary result even when the helper is referenced only once syntactically
-- inside the recursive term. A helper whose body is a top-level set operation is out of scope
-- and still raises `UNSUPPORTED_METHOD`; the recursive CTE's own rejection is covered by
-- `03927_recursive_materialized_cte`.
--
-- Temporary table names created for a materialized CTE are random, so raw `EXPLAIN` output is
-- never pinned directly; instead we count how many
-- `MaterializingCTE (Materializing CTE: <name>)` plan-step lines the query plan has.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- A. Plain materialized helper referenced through an `IN` subquery from the recursive term.
WITH RECURSIVE evens AS MATERIALIZED (SELECT number * 2 AS n FROM numbers(10)),
search AS (SELECT 0 AS x UNION ALL SELECT x + 2 FROM search WHERE x < 8 AND (x + 2) IN (SELECT n FROM evens))
SELECT * FROM search ORDER BY x;

-- B. Plain materialized helper referenced through a `JOIN` from the recursive term.
WITH RECURSIVE seq AS MATERIALIZED (SELECT number AS n FROM numbers(1, 5)),
walk AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM walk INNER JOIN seq ON seq.n = walk.x WHERE x < 5)
SELECT * FROM walk ORDER BY x;

-- C1. The helper is referenced exactly once, inside the recursive term only. The static
--     reference count is one, so only the analysis marker can keep it materialized.
--     Expected: exactly one materialization step.
SELECT count() FROM (
    EXPLAIN
    WITH RECURSIVE helper AS MATERIALIZED (SELECT rand() AS r),
    walk AS (SELECT toUInt64(0) AS n, toUInt32(0) AS r UNION ALL SELECT n + 1, helper.r FROM walk CROSS JOIN helper WHERE n < 4)
    SELECT * FROM walk
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE: helper)%';

-- C2. Functional counterpart of C1: since the helper is materialized once, every recursive
--     step observes the same random value.
WITH RECURSIVE helper AS MATERIALIZED (SELECT rand() AS r),
walk AS (SELECT toUInt64(0) AS n, toUInt32(0) AS r UNION ALL SELECT n + 1, helper.r FROM walk CROSS JOIN helper WHERE n < 4)
SELECT uniqExactIf(r, n > 0) FROM walk;

-- C3. A helper referenced only in the seed term is executed once, so it stays single-use and
--     is still inlined. Expected: zero materialization steps. Pins that marking is applied to
--     `queries[1..]` only and never to the seed.
SELECT count() FROM (
    EXPLAIN
    WITH RECURSIVE helper AS MATERIALIZED (SELECT toUInt64(0) AS n),
    walk AS (SELECT n FROM helper UNION ALL SELECT n + 1 FROM walk WHERE n < 3)
    SELECT * FROM walk
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE: helper)%';

-- D. Chained materialized helpers: `filtered` is read from the recursive term, `base` is read
--    only from `filtered`'s own body. A materialized CTE's body is evaluated once while its
--    temporary table is populated, so the repeated-context marking must stop at that boundary
--    and `base` must stay inlined. Expected: exactly one materialization step, for `filtered`.
SELECT count() FROM (
    EXPLAIN
    WITH RECURSIVE
    base AS MATERIALIZED (SELECT number AS n FROM numbers(5)),
    filtered AS MATERIALIZED (SELECT n FROM base WHERE n % 2 = 0),
    walk AS (SELECT toUInt64(0) AS x UNION ALL SELECT x + 2 FROM walk INNER JOIN filtered ON filtered.n = walk.x + 2 WHERE x < 4)
    SELECT * FROM walk
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE: %';

WITH RECURSIVE
base AS MATERIALIZED (SELECT number AS n FROM numbers(5)),
filtered AS MATERIALIZED (SELECT n FROM base WHERE n % 2 = 0),
walk AS (SELECT toUInt64(0) AS x UNION ALL SELECT x + 2 FROM walk INNER JOIN filtered ON filtered.n = walk.x + 2 WHERE x < 4)
SELECT * FROM walk ORDER BY x;

-- E. The helper has exactly one reference site and it is in the SECOND recursive branch.
--    `RecursiveCTESource` re-executes all of `queries[1..]` on every step, so the helper must
--    still be materialized once. Pins that marking covers every recursive member, not only the
--    first one.
SELECT count() FROM (
    EXPLAIN
    WITH RECURSIVE helper AS MATERIALIZED (SELECT rand() AS r),
    walk AS
    (
        SELECT toUInt64(0) AS n, toUInt32(0) AS r
        UNION ALL
        SELECT n + 1, r FROM walk WHERE n < 3
        UNION ALL
        SELECT n + 100, helper.r FROM walk CROSS JOIN helper WHERE n < 3 AND n > 0
    )
    SELECT * FROM walk
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE: helper)%';

WITH RECURSIVE helper AS MATERIALIZED (SELECT rand() AS r),
walk AS
(
    SELECT toUInt64(0) AS n, toUInt32(0) AS r
    UNION ALL
    SELECT n + 1, r FROM walk WHERE n < 3
    UNION ALL
    SELECT n + 100, helper.r FROM walk CROSS JOIN helper WHERE n < 3 AND n > 0
)
SELECT uniqExactIf(r, n >= 100), groupArraySorted(10)(n) FROM walk;

-- F. A recursive CTE nested inside a materialized helper, whose own helper is referenced from the
--    inner recursive term. The inner recursive union marks `inner_helper` independently of the
--    outer context, so every inner iteration reads the same value. This shape is rejected outright
--    before this change, so the case pins that a plain `SELECT` helper declared `MATERIALIZED` in a
--    nested `WITH RECURSIVE` is accepted; it is not sensitive to the marking itself, because the
--    inner plan keeps `inner_helper` materialized either way.
WITH outer_helper AS MATERIALIZED
(
    WITH RECURSIVE inner_helper AS MATERIALIZED (SELECT rand() AS r),
    inner_walk AS
    (
        SELECT toUInt64(0) AS n, toUInt32(0) AS r
        UNION ALL
        SELECT n + 1, inner_helper.r FROM inner_walk CROSS JOIN inner_helper WHERE n < 3
    )
    SELECT uniqExactIf(r, n > 0) AS distinct_r FROM inner_walk
)
SELECT distinct_r FROM outer_helper UNION ALL SELECT distinct_r FROM outer_helper;

-- G. Negative: a helper whose body is a top-level set operation is out of scope for this
--    implementation. At query tree build time it is indistinguishable from the recursive CTE
--    itself, which is why both are rejected the same way.
WITH RECURSIVE steps AS MATERIALIZED (SELECT toUInt64(1) AS step UNION ALL SELECT toUInt64(2) AS step),
walk AS (SELECT toUInt64(0) AS x UNION ALL SELECT x + step FROM walk CROSS JOIN steps WHERE x < 2)
SELECT * FROM walk ORDER BY x; -- { serverError UNSUPPORTED_METHOD }

-- H. That rejection is unconditional: it does not depend on `enable_materialized_cte`.
SET enable_materialized_cte = 0;
WITH RECURSIVE steps AS MATERIALIZED (SELECT toUInt64(1) AS step UNION ALL SELECT toUInt64(2) AS step),
walk AS (SELECT toUInt64(0) AS x UNION ALL SELECT x + step FROM walk CROSS JOIN steps WHERE x < 2)
SELECT * FROM walk ORDER BY x; -- { serverError UNSUPPORTED_METHOD }
