-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101671
-- This test guards against a past regression where `convertAnyJoinToSemiOrAntiJoin`
-- incorrectly converted ANY LEFT JOIN to ANTI JOIN.
--
-- Historically, `filterResultForMatchedRows` evaluated the filter via dry-run without
-- a not-ready-set guard, so `FunctionIn` returned zeros for a not-ready set and produced
-- a concrete FALSE (instead of UNKNOWN), triggering the bogus ANTI conversion. The sibling
-- `filterResultForNotMatchedRows` already checked for not-ready sets. Master now guards both
-- paths with `dagContainsNonReadySet` before dry-run evaluation.

DROP TABLE IF EXISTS t1_04305;
DROP TABLE IF EXISTS t2_04305;

CREATE TABLE t1_04305 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2_04305 (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1_04305 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2_04305 VALUES (1, 100), (2, 200);

-- Pin the conversion on; the runner randomizes it and 0 would skip the
-- conversion under test, passing vacuously. enable_analyzer must also be on:
-- the old-analyzer config disables convertAnyJoinToSemiOrAntiJoin entirely.
SET enable_analyzer = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;

-- Case 1: The filter toUInt64(1 IN (SELECT number FROM numbers(10))) > 0 is
-- always TRUE at runtime. With the historical bug, `FunctionIn` dry-run returns 0 for
-- the not-ready set, `filterResultForMatchedRows` returns FALSE, and the optimizer
-- incorrectly converts ANY LEFT JOIN to ANTI JOIN, dropping all matched rows.
-- Expected: all 3 rows (matched a=1,2 + unmatched a=3).
SELECT a, b, c
FROM t1_04305
ANY LEFT JOIN t2_04305 USING (a)
WHERE toUInt64(1 IN (SELECT number FROM numbers(10))) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0;

-- Case 2: Same bug, but with globalIn, where the set might not be ready even
-- in the subquery pipeline stage. Same incorrect ANTI conversion expected.
SELECT a, b, c
FROM t1_04305
ANY LEFT JOIN t2_04305 USING (a)
WHERE toUInt64(1 GLOBAL IN (SELECT number FROM numbers(10))) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0;

-- Case 3: NOT IN variant. 1 NOT IN (0,1,...,9) = FALSE, so toUInt64(0) > 0
-- is FALSE, and no rows should be returned. This verifies the optimization
-- doesn't break the NOT IN path.
SELECT a, b, c
FROM t1_04305
ANY LEFT JOIN t2_04305 USING (a)
WHERE toUInt64(1 NOT IN (SELECT number FROM numbers(10))) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0;

DROP TABLE t1_04305;
DROP TABLE t2_04305;
