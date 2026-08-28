-- Pins that a non-recursive helper CTE with a plain `SELECT` body declared `MATERIALIZED`
-- inside a `WITH RECURSIVE` clause is materialized once, and that every recursive iteration
-- reads the same temporary result even when the helper is referenced only once syntactically
-- inside the recursive term. The same rule applies to a materialized CTE declared in an
-- enclosing `WITH` and read from a recursive member, because it is about where a reference
-- executes rather than where the CTE is declared. A helper whose body is a top-level set
-- operation is out of scope and still raises `UNSUPPORTED_METHOD`, and so is a helper whose own
-- definition reads the recursive CTE; the recursive CTE's own rejection is covered by
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

-- G. A materialized CTE declared in an enclosing, non-recursive `WITH` and read once from an
--    inner recursive term. This shape is accepted before this change too, and the helper was
--    re-evaluated on every recursion step; it is now materialized once, because the marking
--    follows where a reference executes and not where the CTE is declared. Pins that the marking
--    is not narrowed to CTEs declared inside the `WITH RECURSIVE` clause.
SELECT count() FROM (
    EXPLAIN
    WITH h AS MATERIALIZED (SELECT rand() AS r)
    SELECT * FROM
    (
        WITH RECURSIVE walk AS
        (
            SELECT toUInt64(0) AS n, toUInt32(0) AS r
            UNION ALL
            SELECT n + 1, h.r FROM walk CROSS JOIN h WHERE n < 3
        )
        SELECT * FROM walk
    )
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE: h)%';

WITH h AS MATERIALIZED (SELECT rand() AS r)
SELECT uniqExactIf(r, n > 0) FROM
(
    WITH RECURSIVE walk AS
    (
        SELECT toUInt64(0) AS n, toUInt32(0) AS r
        UNION ALL
        SELECT n + 1, h.r FROM walk CROSS JOIN h WHERE n < 3
    )
    SELECT * FROM walk
);

-- H. Negative: a materialized helper whose own definition reads the recursive CTE. Inside a
--    recursive member the recursive CTE's name resolves to the step-local working table, so a
--    single materialization would freeze the first step's contents and change the fixed point.
--    Rejected, rather than silently returning a different result than the same query without
--    `MATERIALIZED`. H2 has two reference sites, so the ordinary use count would materialize the
--    helper even if it were merely left unmarked: the rejection has to be explicit. H3 hides the
--    read one materialized CTE deeper.
WITH RECURSIVE helper AS MATERIALIZED (SELECT n + 1 AS n FROM walk),
walk AS (SELECT toUInt64(0) AS n UNION ALL SELECT n FROM helper WHERE n < 3)
SELECT * FROM walk ORDER BY n; -- { serverError UNSUPPORTED_METHOD }

WITH RECURSIVE helper AS MATERIALIZED (SELECT n + 1 AS n FROM walk),
walk AS (SELECT toUInt64(0) AS n UNION ALL SELECT h1.n FROM helper AS h1 CROSS JOIN helper AS h2 WHERE h1.n < 3 AND h2.n < 3)
SELECT * FROM walk ORDER BY n; -- { serverError UNSUPPORTED_METHOD }

WITH RECURSIVE helper AS MATERIALIZED (WITH inner_h AS MATERIALIZED (SELECT n + 1 AS n FROM walk) SELECT n FROM inner_h),
walk AS (SELECT toUInt64(0) AS n UNION ALL SELECT n FROM helper WHERE n < 3)
SELECT * FROM walk ORDER BY n; -- { serverError UNSUPPORTED_METHOD }

-- H4. The same rejection when the helper is declared in an enclosing `WITH`, since the rule follows
--     where a reference executes rather than where the CTE is declared. This shape is accepted
--     before this change: with one reference site the helper happened to be inlined and so gave the
--     right answer, but with two reference sites the ordinary use count materialized it and it
--     silently returned `0` instead of `0, 1, 2`. Both are rejected now.
WITH h AS MATERIALIZED (SELECT n + 1 AS n FROM walk)
SELECT * FROM
(
    WITH RECURSIVE walk AS (SELECT toUInt64(0) AS n UNION ALL SELECT n FROM h WHERE n < 3)
    SELECT * FROM walk
)
ORDER BY n; -- { serverError UNSUPPORTED_METHOD }

WITH h AS MATERIALIZED (SELECT n + 1 AS n FROM walk)
SELECT * FROM
(
    WITH RECURSIVE walk AS (SELECT toUInt64(0) AS n UNION ALL SELECT h1.n FROM h AS h1 CROSS JOIN h AS h2 WHERE h1.n < 3 AND h2.n < 3)
    SELECT * FROM walk
)
ORDER BY n; -- { serverError UNSUPPORTED_METHOD }

-- I. Controls for `H`, so the rejection cannot grow beyond the shape it is meant to catch. Without
--    `MATERIALIZED` the same mutual reference is an ordinary CTE and still works -- including from an
--    enclosing `WITH` -- a materialized CTE that reads a recursive CTE from outside its recursive
--    members is untouched, because there it reads the finished fixed point rather than the
--    step-local working table, and the rejection itself is conditional on the setting.
WITH RECURSIVE helper AS (SELECT n + 1 AS n FROM walk),
walk AS (SELECT toUInt64(0) AS n UNION ALL SELECT n FROM helper WHERE n < 3)
SELECT * FROM walk ORDER BY n;

WITH h AS (SELECT n + 1 AS n FROM walk)
SELECT * FROM
(
    WITH RECURSIVE walk AS (SELECT toUInt64(0) AS n UNION ALL SELECT n FROM h WHERE n < 3)
    SELECT * FROM walk
)
ORDER BY n;

WITH RECURSIVE walk AS (SELECT toUInt64(0) AS n UNION ALL SELECT n + 1 FROM walk WHERE n < 3),
helper AS MATERIALIZED (SELECT sum(n) AS s FROM walk)
SELECT s FROM helper;

-- I4. Unlike the two syntactic rejections in `J` below, `H` depends on `enable_materialized_cte`:
--     with the setting off `MATERIALIZED` is ignored, the CTE is an ordinary one, and the query runs.
WITH RECURSIVE helper AS MATERIALIZED (SELECT n + 1 AS n FROM walk),
walk AS (SELECT toUInt64(0) AS n UNION ALL SELECT n FROM helper WHERE n < 3)
SELECT * FROM walk ORDER BY n SETTINGS enable_materialized_cte = 0;

-- J. Negative: a helper whose body is a top-level set operation is out of scope for this
--    implementation. At query tree build time it is indistinguishable from the recursive CTE
--    itself, which is why both are rejected the same way.
WITH RECURSIVE steps AS MATERIALIZED (SELECT toUInt64(1) AS step UNION ALL SELECT toUInt64(2) AS step),
walk AS (SELECT toUInt64(0) AS x UNION ALL SELECT x + step FROM walk CROSS JOIN steps WHERE x < 2)
SELECT * FROM walk ORDER BY x; -- { serverError UNSUPPORTED_METHOD }

-- K. That rejection is unconditional: it does not depend on `enable_materialized_cte`.
SET enable_materialized_cte = 0;
WITH RECURSIVE steps AS MATERIALIZED (SELECT toUInt64(1) AS step UNION ALL SELECT toUInt64(2) AS step),
walk AS (SELECT toUInt64(0) AS x UNION ALL SELECT x + step FROM walk CROSS JOIN steps WHERE x < 2)
SELECT * FROM walk ORDER BY x; -- { serverError UNSUPPORTED_METHOD }
