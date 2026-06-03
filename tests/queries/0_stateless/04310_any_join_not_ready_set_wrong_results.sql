-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100029
-- ANY LEFT JOIN with a filter containing IN (subquery) produces wrong results
-- when query_plan_filter_push_down=0 keeps the filter above JoinStepLogical.
--
-- Root cause: convertAnyJoinToSemiOrAntiJoin's filterResultForMatchedRows
-- evaluates the filter via dry-run where FunctionIn returns zeros for not-ready
-- sets, producing a concrete FALSE instead of UNKNOWN. This causes incorrect
-- ANTI JOIN conversion, silently dropping matched rows.
--
-- Expected: all 3 rows (filter is always TRUE at runtime).

DROP TABLE IF EXISTS t_left_04310;
DROP TABLE IF EXISTS t_right_04310;
DROP TABLE IF EXISTS t_subq_04310;

CREATE TABLE t_left_04310 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_right_04310 (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_subq_04310 (x UInt64) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_left_04310 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t_right_04310 VALUES (1, 100), (2, 200);
INSERT INTO t_subq_04310 VALUES (1);

-- Case 1: ANY LEFT JOIN with IN subquery from a real table.
-- The filter toUInt64(1 IN (SELECT x FROM t_subq_04310)) > 0 evaluates to TRUE
-- at runtime (1 IS in the subquery result), so all rows should pass.
SELECT a, b, c
FROM t_left_04310
ANY LEFT JOIN t_right_04310 USING (a)
WHERE toUInt64(1 IN (SELECT x FROM t_subq_04310)) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0,
         query_plan_split_filter = 0,
         query_plan_merge_filters = 0;

-- Case 2: Same with numbers() system table — no real table subquery needed.
SELECT a, b, c
FROM t_left_04310
ANY LEFT JOIN t_right_04310 USING (a)
WHERE toUInt64(1 IN (SELECT number FROM numbers(10))) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0;

-- Case 3: ANY RIGHT JOIN variant — uses the same code path (JoinKind::Right).
SELECT a, b, c
FROM t_left_04310
ANY RIGHT JOIN t_right_04310 USING (a)
WHERE toUInt64(1 IN (SELECT number FROM numbers(10))) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0;

DROP TABLE t_left_04310;
DROP TABLE t_right_04310;
DROP TABLE t_subq_04310;
