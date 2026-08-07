-- Tags: no-parallel-replicas

-- Parallel replicas re-serialize the `ON` clause as SQL for the remote legs, where the opposite side
-- is a temporary table instead of a plan-time constant. Nothing is substitutable there, so the plan
-- these assertions describe never forms. Measured identical on master, so nothing is lost.

-- The analyzer is required: the logical join step this test exercises only exists there, and a
-- `compatibility` draw below 24.3 would otherwise silently turn it off and make every assertion pass
-- for the wrong reason.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_04695;
DROP TABLE IF EXISTS t_04695_granules;
DROP TABLE IF EXISTS t_04695_bounds;

-- index_granularity is pinned because the assertions below count parts and granules.
CREATE TABLE t_04695 (d Date, v UInt32)
ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d
SETTINGS index_granularity = 8192;
INSERT INTO t_04695 SELECT toDate('2025-01-01') + number, number FROM numbers(400);

-- A second fixture for the granule-level assertion. Above, every monthly part is a single granule, so
-- its `Granules: N/M` line merely restates `Parts: N/M`. Here all 400 rows land in ONE part of 25
-- granules, so a primary-key decline at granule level cannot hide behind partition pruning.
CREATE TABLE t_04695_granules (d Date, v UInt32)
ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 16;
INSERT INTO t_04695_granules SELECT toDate('2025-01-01') + number, number FROM numbers(400);

-- A one-row table is NOT a plan-time constant, so it must stay unpruned (see the last section).
CREATE TABLE t_04695_bounds (lo Date, hi Date) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04695_bounds VALUES ('2025-06-01', '2025-06-10');

SELECT '-- bounds supplied through JOIN ON must reach index analysis';

-- Two-sided range. Reads 1 of 14 parts.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
    SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
) WHERE explain LIKE '%Parts: 1/14%';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi;

SELECT '-- granules, not only parts';

-- Granule pruning has to be asserted on its own: `describeIndexes` reports the part and granule
-- counts independently, so a primary-key decline at granule level would leave a `Parts:` assertion
-- green. On the single-part fixture the key must select 2 of the 25 granules.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
    SELECT count() FROM t_04695_granules AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
) WHERE explain LIKE '%Granules: 2/25%';

-- The same predicate as a plain `WHERE`, which pruned on every version. It reads the same granules,
-- so the row above is not asserting a bound that only the JOIN form could reach.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_04695_granules AS t
    WHERE t.d >= toDate('2025-06-01') AND t.d <= toDate('2025-06-10')
) WHERE explain LIKE '%Granules: 2/25%';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695_granules AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi;

SELECT '-- Nullable bounds';

SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toNullable(toDate('2025-06-01')) AS lo, toNullable(toDate('2025-06-10')) AS hi)
    SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
) WHERE explain LIKE '%Parts: 1/14%';

WITH bounds AS (SELECT toNullable(toDate('2025-06-01')) AS lo, toNullable(toDate('2025-06-10')) AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi;

SELECT '-- LowCardinality bounds';

SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toLowCardinality(toDate('2025-06-01')) AS lo, toLowCardinality(toDate('2025-06-10')) AS hi)
    SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
) WHERE explain LIKE '%Parts: 1/14%';

WITH bounds AS (SELECT toLowCardinality(toDate('2025-06-01')) AS lo, toLowCardinality(toDate('2025-06-10')) AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi;

SELECT '-- single-sided range prunes partially (9 of 14 parts, not 1)';

SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo)
    SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo
) WHERE explain LIKE '%Parts: 9/14%';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo;

SELECT '-- strict inequalities';

SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
    SELECT count() FROM t_04695 AS t JOIN bounds ON t.d > bounds.lo AND t.d < bounds.hi
) WHERE explain LIKE '%Parts: 1/14%';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d > bounds.lo AND t.d < bounds.hi;

SELECT '-- an expression over the key prunes through the partition key';

SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
    SELECT count() FROM t_04695 AS t JOIN bounds ON toYYYYMM(t.d) >= toYYYYMM(bounds.lo) AND toYYYYMM(t.d) <= toYYYYMM(bounds.hi)
) WHERE explain LIKE '%Parts: 1/14%';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON toYYYYMM(t.d) >= toYYYYMM(bounds.lo) AND toYYYYMM(t.d) <= toYYYYMM(bounds.hi);

SELECT '-- the same bounds on the left of the join';

SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
    SELECT count() FROM bounds JOIN t_04695 AS t ON t.d >= bounds.lo AND t.d <= bounds.hi
) WHERE explain LIKE '%Parts: 1/14%';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM bounds JOIN t_04695 AS t ON t.d >= bounds.lo AND t.d <= bounds.hi;

SELECT '-- a user-written name shared by both sides still answers correctly';

-- The SOURCE-RELATION and by-POSITION contracts are NOT asserted here: the analyzer qualifies these
-- columns to `__table1.x` / `__table2.x`, so no collision is reachable from SQL at the layer the
-- substitution runs. `gtest_join_on_constant_pushdown.cpp` asserts both by building the step directly.

-- What is covered is the answer for the closest shape SQL can express, a name shared as WRITTEN. Both
-- conjuncts must survive: `x >= 50` and the left-only `y > 90`.
SELECT count() FROM (SELECT number AS x, number AS y FROM numbers(100)) AS l
JOIN (SELECT toUInt64(50) AS x) AS r ON l.x >= r.x AND l.y > 90;

-- The same answer computed without any pushdown available, as an oracle.
SELECT count() FROM (SELECT number AS x, number AS y FROM numbers(100)) AS l
JOIN (SELECT materialize(toUInt64(50)) AS x) AS r ON l.x >= r.x AND l.y > 90;

-- And with the shared name on the KEY column of a real table, so pruning is exercised too.
SELECT count() FROM t_04695 AS t
JOIN (SELECT toDate('2025-06-01') AS d) AS r ON t.d >= r.d AND t.v > 180;

SELECT count() FROM t_04695 AS t
JOIN (SELECT materialize(toDate('2025-06-01')) AS d) AS r ON t.d >= r.d AND t.v > 180;

SELECT '-- an equality against a constant must remain a join key, not become a filter';

-- If the equality were pushed down instead of kept, the join would lose its only key and degrade to a
-- CROSS join, which the `Algorithm` line reports as `ConstantJoin`. Asserting the algorithm rather than
-- the `Join conditions:` line survives `query_plan_convert_join_to_in` and `query_plan_join_swap_table`.

-- `actions = 1` is required and NOT implied by `indexes = 1`: the line comes from `describeJoinActions`,
-- reached only under `options.actions`, which is force-enabled only when `explain_query_plan_default` is
-- `pretty` -- a default that flipped in 26.7 and that stress runs randomize through `compatibility`.
SELECT count() = 0 FROM (
    EXPLAIN indexes = 1, actions = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
    SELECT count() FROM t_04695 AS t JOIN bounds ON t.d = bounds.lo AND t.d <= bounds.hi
) WHERE explain ILIKE '%ConstantJoin%';

-- The positive half of the same claim: a join `Algorithm` line must actually be printed, so a plan
-- printing none cannot satisfy the negative assertion above by silence. The `Join` suffix is required
-- because `describeIndexes` prints an unrelated `Search Algorithm:` line that would otherwise match.

-- The concrete algorithm is deliberately NOT asserted: randomized settings rename this line while
-- leaving a real keyed join (`ConcurrentHashJoin`, `SpillingHashJoin`, `GraceHashJoin`,
-- `PartialMergeJoin`, `JoinSwitcher`). Only `ConstantJoin` is the degradation under test.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1, actions = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
    SELECT count() FROM t_04695 AS t JOIN bounds ON t.d = bounds.lo AND t.d <= bounds.hi
    SETTINGS query_plan_convert_join_to_in = 0
) WHERE explain ILIKE '%Algorithm: %Join%';

-- The range predicate sitting next to the key is still pushed down. Pinned to the plan-based join
-- because `query_plan_convert_join_to_in` rewrites the equality into an `IN` and reads all parts.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
    SELECT count() FROM t_04695 AS t JOIN bounds ON t.d = bounds.lo AND t.d <= bounds.hi
    SETTINGS query_plan_convert_join_to_in = 0
) WHERE explain LIKE '%Parts: 6/14%';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d = bounds.lo AND t.d <= bounds.hi;

SELECT '-- results are unchanged where the value is not a plan-time constant';

-- A one-row MergeTree table is not a constant: the bounds are unknown until the join runs, so the
-- predicate must stay above the join and the left side stays unpruned. All three `Parts: 14/14` lines
-- are counted, because an existence test would be satisfied by any one index, not the PRIMARY KEY.
SELECT count() = 3 FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_04695 AS t JOIN t_04695_bounds AS b ON t.d >= b.lo AND t.d <= b.hi
) WHERE explain LIKE '%Parts: 14/14%';

SELECT count() = 3 FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_04695 AS t JOIN t_04695_bounds AS b ON t.d >= b.lo AND t.d <= b.hi
) WHERE explain LIKE '%Condition: true%';

SELECT count() FROM t_04695 AS t JOIN t_04695_bounds AS b ON t.d >= b.lo AND t.d <= b.hi;

-- A right side selecting no rows yields no output rows, whether or not the left side is pruned.
SELECT count() FROM t_04695 AS t
JOIN (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi WHERE 0) AS b
ON t.d >= b.lo AND t.d <= b.hi;

-- A right side with more than one row is not a constant either.
SELECT count() FROM t_04695 AS t
JOIN (SELECT toDate('2025-06-01') AS lo UNION ALL SELECT toDate('2025-07-01')) AS b
ON t.d >= b.lo;

SELECT '-- every join algorithm now answers a relocatable inequality-only ON against a constant side';

-- Extracting the whole `ON` clause empties `join_operator.expression`, making this a join-with-constant
-- that skips key derivation, which is what previously rejected an inequality-only `ON`: finding no keys
-- it raised INVALID_JOIN_ON_EXPRESSION unless hash was enabled. These rows assert the ANSWER.

-- The first four fail without the fix. The `hash` row is a CONTROL that answers on BOTH arms and must
-- keep doing so, because the CROSS-join fallback is reachable exactly when hash is enabled, so this
-- section is not all-new behaviour.
WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
SETTINGS join_algorithm = 'partial_merge';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
SETTINGS join_algorithm = 'full_sorting_merge';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
SETTINGS join_algorithm = 'grace_hash';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
SETTINGS join_algorithm = 'parallel_hash';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
SETTINGS join_algorithm = 'hash';

SELECT '-- a disjunction of equalities stays a join key, so non-hash algorithms still reject it';

-- Each disjunct becomes its own join clause and needs a key. Extracting the disjunction as a filter
-- would make these succeed on algorithms that do not implement multiple key clauses.
SET join_algorithm = 'partial_merge';
SELECT 1 FROM (SELECT 1 AS a) AS l JOIN (SELECT 1 AS b, 1 AS c) AS r ON a = b OR a = c; -- { serverError NOT_IMPLEMENTED }
SET join_algorithm = 'grace_hash';
SELECT 1 FROM (SELECT 1 AS a) AS l JOIN (SELECT 1 AS b, 1 AS c) AS r ON a = b OR a = c; -- { serverError NOT_IMPLEMENTED }
SET join_algorithm = 'default';

SELECT '-- outer and non-ALL strictness keep rejecting an inequality-only ON';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t LEFT JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi; -- { serverError INVALID_JOIN_ON_EXPRESSION }

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t FULL JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi; -- { serverError INVALID_JOIN_ON_EXPRESSION }

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count() FROM t_04695 AS t ANY INNER JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '-- rows, not only counts';

WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-05') AS hi)
SELECT t.d FROM t_04695 AS t JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi ORDER BY t.d;

SELECT '-- a stateful or non-deterministic ON predicate is not moved below the join';

-- The join changes the row set AND the block partitioning, which is what these predicates depend on,
-- so substituting the constant and evaluating below the join would change the answer. `trySplitFilter`
-- refuses such a predicate for the ordinary pushdown path, and this section pins the same refusal here.

-- The observable is WHICH filter step holds the predicate: a `Join filter` step is the one produced
-- below the join. It stays absent for both rows, while the ordinary shapes above still produce it, so
-- this is not asserting that pushdown stopped working in general.

-- `join_algorithm` is pinned because a declined predicate leaves an inequality-only `ON`, which the
-- non-hash algorithms reject outright with INVALID_JOIN_ON_EXPRESSION, and stress runs randomize it.

-- Stateful: `rowNumberInBlock` reads its position within the block it is evaluated in.
SELECT count() = 0 FROM (
    EXPLAIN
    SELECT count() FROM t_04695 AS t
    JOIN (SELECT toDate('2025-06-01') AS lo) AS b ON t.d >= b.lo + rowNumberInBlock()
    SETTINGS join_algorithm = 'hash'
) WHERE explain ILIKE '%Join filter%';

-- Not deterministic in scope of query, and NOT stateful, so it covers the second half of the guard
-- independently: `rand` is `isDeterministicInScopeOfQuery() = false` while `isStateful() = false`.
SELECT count() = 0 FROM (
    EXPLAIN
    SELECT count() FROM t_04695 AS t
    JOIN (SELECT toDate('2025-06-01') AS lo) AS b ON t.d >= b.lo + (rand() % 1)
    SETTINGS join_algorithm = 'hash'
) WHERE explain ILIKE '%Join filter%';

-- `rand() % 1` is always 0, so the bound is the same as the plain one and the ANSWER is assertable too.
SELECT count() FROM t_04695 AS t
JOIN (SELECT toDate('2025-06-01') AS lo) AS b ON t.d >= b.lo + (rand() % 1)
SETTINGS join_algorithm = 'hash';

SELECT count() FROM t_04695 AS t
JOIN (SELECT materialize(toDate('2025-06-01')) AS lo) AS b ON t.d >= b.lo + (rand() % 1)
SETTINGS join_algorithm = 'hash';

SELECT '-- a row-count-changing ON predicate is not moved below the join';

-- `arrayJoin` is a carrier the statefulness and determinism checks above cannot see: it is its own
-- kind of plan node rather than a function, so it carries no function metadata to interrogate.
-- Evaluated below the join it re-derives the row count there, duplicating the join's output rows.

-- The inequality is the newly covered axis: an equality between the two relations would become a join
-- key and is declined earlier for that reason, so only an inequality reaches the substitution path.
-- 03918 covers `arrayJoin` in `ON` for equalities only.

-- The observable is PLACEMENT, as in the section above: a `Join filter` step is the one produced below
-- the join, and it must stay absent. The answer alone is not an observable here - `arrayJoin` in `ON`
-- already expands rows above the join, so the row count happens to agree either way for the shapes
-- reachable today, and asserting only the answer would pass against an unguarded build.
SELECT count() = 0 FROM (
    EXPLAIN
    SELECT arrayJoin(l.a) FROM (SELECT [1, 2] AS a) AS l
    JOIN (SELECT 1 AS c) AS r ON arrayJoin(l.a) >= r.c
    SETTINGS join_algorithm = 'hash'
) WHERE explain ILIKE '%Join filter%';

-- The answer is pinned alongside it so that a future placement change cannot silently alter the result.
SELECT arrayJoin(l.a) FROM (SELECT [1, 2] AS a) AS l
JOIN (SELECT 1 AS c) AS r ON arrayJoin(l.a) >= r.c
ORDER BY 1
SETTINGS join_algorithm = 'hash';

-- The equality control: declined as key-forming either way, so it answers identically on both arms and
-- shows the rows above assert the inequality axis rather than restating this one.
SELECT arrayJoin(l.a) FROM (SELECT [1, 2] AS a) AS l
JOIN (SELECT 1 AS c) AS r ON arrayJoin(l.a) = r.c
ORDER BY 1;

SELECT '-- a throwing ON predicate is not moved below the join';

-- Below the join the predicate also sees the rows a surviving key rejects, so a partial function would
-- raise on a row that never reaches the output. Here `t.k = b.k` keeps only `t.x = 1`, but the moved
-- `intDiv(1, t.x)` would also divide by the unmatched `t.x = 0`.

-- The ANSWER is the observable, because it is what a user sees change: the query must keep answering
-- rather than raising. Both `Parts:` and placement stay identical to master for a declined predicate,
-- so an unguarded build fails this row with ILLEGAL_DIVISION.
SELECT count()
FROM (SELECT number AS k, number AS x FROM numbers(2)) AS t
JOIN (SELECT toUInt64(1) AS k, toUInt64(0) AS lo) AS b
ON t.k = b.k AND intDiv(1, t.x) >= b.lo;

-- The single-sided control: the same predicate with no reference to `b` never enters the substitution
-- path, and master already evaluates it below the join. It must keep raising, so the guard is not
-- silently suppressing exceptions the user is meant to get.
SELECT count()
FROM (SELECT number AS k, number AS x FROM numbers(2)) AS t
JOIN (SELECT toUInt64(1) AS k, toUInt64(0) AS lo) AS b
ON t.k = b.k AND intDiv(1, t.x) >= 1; -- { serverError ILLEGAL_DIVISION }

-- The total-function control: an ON predicate over both relations that cannot throw is still moved, so
-- the rows above narrow the guard to partial functions instead of disabling the optimization.
SELECT count() = 1 FROM (
    EXPLAIN
    SELECT count() FROM t_04695 AS t
    JOIN (SELECT toDate('2025-06-01') AS lo) AS b ON t.d >= b.lo
    SETTINGS join_algorithm = 'hash'
) WHERE explain ILIKE '%Join filter%';

-- A function is admitted only once its totality is established, so one that cannot throw but has not
-- been reviewed is declined as well. That costs only the optimization, so the row is pinned as an
-- answer on `hash` and the pre-existing keyless-ON error elsewhere.
SELECT count()
FROM (SELECT number AS d FROM numbers(2)) AS t
JOIN (SELECT toUInt64(0) AS lo) AS b ON cityHash64(t.d) >= b.lo
SETTINGS join_algorithm = 'hash';

SELECT count()
FROM (SELECT number AS d FROM numbers(2)) AS t
JOIN (SELECT toUInt64(0) AS lo) AS b ON cityHash64(t.d) >= b.lo
SETTINGS join_algorithm = 'partial_merge'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- A function that raises on an out-of-range value must be declined even though it reports itself as
-- not worth evaluating lazily, so that marker cannot stand in for throw-safety. Here the unparsable
-- date sits on the row `t.k = b.k` rejects, and the query must keep answering rather than raising.
SELECT count()
FROM (SELECT number AS k, if(number = 0, 'not-a-date', '2025-01-01') AS s FROM numbers(2)) AS t
JOIN (SELECT toUInt64(1) AS k, toDateTime64('2000-01-01 00:00:00', 3) AS lo) AS b
ON t.k = b.k AND addDays(t.s, 1) >= b.lo;

-- Equal argument types are not enough when the type compares what it CONTAINS: two `Variant`
-- columns meet here at a row with no supertype, which raises rather than answering.
SELECT count()
FROM (SELECT number AS k, CAST(if(number = 1, toUInt64(1), 'bad'), 'Variant(UInt64, String)') AS v FROM numbers(2)) AS t
JOIN (SELECT toUInt64(1) AS k, CAST(toUInt64(1), 'Variant(UInt64, String)') AS lo) AS b
ON t.k = b.k AND t.v >= b.lo;

DROP TABLE t_04695;
DROP TABLE t_04695_granules;
DROP TABLE t_04695_bounds;
