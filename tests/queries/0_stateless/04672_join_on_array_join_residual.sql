-- `arrayJoin` inside a mixed (non equi) JOIN ON condition changes the number of rows, while
-- `buildAdditionalFilter` evaluates that condition per probe batch and indexes the resulting
-- mask by row position. Such a condition is rejected instead of producing an out of bounds
-- read (logical error in debug and sanitizer builds) or silently wrong results.

SET enable_analyzer = 1;

-- The stress job appends two client options from one block, and a client option beats a
-- randomized one (`TestCase.add_effective_settings` in tests/clickhouse-test):
--   * `join_algorithm` on odd threads (ci/jobs/scripts/stress/stress.py:166-179). With
--     `partial_merge`, `full_sorting_merge` or `auto` a mixed condition is refused before a
--     `HashJoin` is built (src/Planner/PlannerJoins.cpp:1386-1392), so the rows below would
--     report NOT_IMPLEMENTED instead of exercising the check added here. The
--     algorithm-matrix rows override this per statement.
--   * `join_use_nulls=1` on every third thread (ci/jobs/scripts/stress/stress.py:163-164).
--     That makes the non-preserved side of an outer join Nullable
--     (src/Processors/QueryPlan/JoinStepLogical.cpp:113-133), so `sum` over zero matched
--     rows returns NULL rather than 0 and the numeric expectations below would not match.
--     It also makes the `Join` engine row at the end fail outright, because `StorageJoin`
--     requires the query's setting to agree with the one captured when the table was
--     created (src/Storages/StorageJoin.cpp:248-252); that is pre-existing behaviour,
--     unrelated to this change. The rejected rows are unaffected either way: the residual
--     decision at src/Processors/QueryPlan/JoinStepLogical.cpp:1288 does not read this
--     setting.
-- The functional runner randomizes neither setting, which is why a 50/50 run cannot see
-- this class.
SET join_algorithm = 'hash';
SET join_use_nulls = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (key String, a UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t1 VALUES ('k1', 1, [1, 2]), ('k1', 2, [3]), ('k1', 3, []);
CREATE TABLE t2 (key String, a UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t2 VALUES ('k1', 10, [5, 6]), ('k1', 20, [7]);

SELECT '--- rejected: cross-side arrayJoin in a mixed JOIN ON condition';

-- Empty array: the mask is shorter than the batch. This is the reported failure.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64())))); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- Multiplying array: the mask is longer than the batch, so flags are paired with the wrong rows.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize([1, 1])))); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- A single element array happens to preserve the number of rows, but the replication factor is
-- only known while the expression runs, so the condition is refused on structure.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize([1])))); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < t2.a + arrayJoin(materialize([0, 0]))); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < t2.a + arrayJoin(materialize(emptyArrayUInt64()))); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- Bilateral: neither side alone can evaluate the argument.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < arrayJoin(arrayConcat(t1.arr, t2.arr))); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- A one-sided conjunct next to a cross-side one does not make the cross-side one safe.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t2.a > arrayJoin([0, 0])) AND (t1.a < t2.a + arrayJoin([0, 0])); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t2.a > arrayJoin([0, 0])) AND (t1.a < t2.a + arrayJoin([0, 0, 0])); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a > arrayJoin([0, 0])) AND (t1.a < t2.a + arrayJoin([0, 0])); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- An equi key on the same `arrayJoin` node does not remove it from the residual either.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.a = arrayJoin(t2.arr)) AND (t1.a < t2.a + arrayJoin(t2.arr)); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- The disjunctive route reaches the same residual by a different code path.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON ((t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))) OR (t1.a = t2.a); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '--- rejected: every strictness and every hash algorithm';

SELECT count() FROM t1 LEFT SEMI JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64())))); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count() FROM t1 LEFT ANTI JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64())))); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count() FROM t1 LEFT ANY JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64())))); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 RIGHT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 FULL JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'parallel_hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 RIGHT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'parallel_hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 FULL JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'parallel_hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'grace_hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 RIGHT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'grace_hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 FULL JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'grace_hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '--- the supported rewrite: ARRAY JOIN in a subquery before the JOIN';

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN
    (SELECT key, a, arrayJoin(materialize([1, 1])) AS aj FROM t2) AS t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, t2.aj));

SELECT '--- kept: one-sided arrayJoin is hoisted out of the residual';

-- Right side only.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t2.a < arrayJoin(t2.arr));
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t2.a < arrayJoin(emptyArrayUInt64()));
-- Left side only.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a < arrayJoin(materialize(emptyArrayUInt64())));
-- Nested, one side only.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t2.a < arrayJoin(arrayJoin(materialize([[1, 2], [3]]))));
-- An equi key over `arrayJoin` alone stays supported.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON t1.a = arrayJoin(t2.arr);

SELECT '--- kept: a mixed condition without arrayJoin still runs';

-- This has a residual filter but preserves the number of rows, so it is unaffected. Removing
-- the `arrayJoin` test from the check would reject this row too.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a < t2.a);
SELECT count(), sum(t2.a) FROM t1 RIGHT JOIN t2 ON (t1.key = t2.key) AND (t1.a < t2.a);
SELECT count(), sum(t2.a) FROM t1 FULL JOIN t2 ON (t1.key = t2.key) AND (t1.a < t2.a);

SELECT '--- kept: INNER pushes the condition down instead of building a residual';

SELECT count(), sum(t2.a) FROM t1 INNER JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))));

SELECT '--- kept: merge and direct algorithms keep reporting NOT_IMPLEMENTED';

-- These never construct a `HashJoin`, so the new check cannot change their error.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'full_sorting_merge'; -- { serverError NOT_IMPLEMENTED }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'partial_merge'; -- { serverError NOT_IMPLEMENTED }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, arrayJoin(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'direct'; -- { serverError NOT_IMPLEMENTED }

SELECT '--- rejected: a merge algorithm listed alongside hash that cannot take the query';

-- A list mixing merge and hash resolves to whichever entry accepts the query, and every entry
-- that would drop the mixed condition instead of evaluating it is refused for a reason that
-- holds independently of this check: hash comes first below, the ON section has two disjuncts
-- (`FullSortingMergeJoin::isSupported` requires `oneDisjunct`), and SEMI is not a strictness
-- `MergeJoinAlgorithm` implements. So a `HashJoin` is built in each case and the check applies.
SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < t2.a + arrayJoin(materialize([0, 0])))
SETTINGS join_algorithm = 'hash,full_sorting_merge'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON ((t1.key = t2.key) AND (t1.a < t2.a + arrayJoin(materialize([0, 0])))) OR (t1.a = t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT count() FROM t1 LEFT SEMI JOIN t2
ON (t1.key = t2.key) AND (t1.a < t2.a + arrayJoin(materialize([0, 0])))
SETTINGS join_algorithm = 'full_sorting_merge,hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '--- rejected: the unnest alias resolves to arrayJoin and is refused too';

SELECT count(), sum(t2.a) FROM t1 LEFT JOIN t2
ON (t1.key = t2.key) AND (t1.a < divide(t2.a, unnest(materialize(emptyArrayUInt64()))))
SETTINGS join_algorithm = 'hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '--- kept: without the analyzer the condition is refused during analysis';


SELECT '--- kept: a plain Join engine table is unaffected';

DROP TABLE IF EXISTS sj;
CREATE TABLE sj (key String, a UInt64) ENGINE = Join(ANY, LEFT, key);
INSERT INTO sj VALUES ('k1', 10);
SELECT count(), sum(sj.a) FROM t1 ANY LEFT JOIN sj USING (key);

DROP TABLE sj;
DROP TABLE t2;
DROP TABLE t1;
