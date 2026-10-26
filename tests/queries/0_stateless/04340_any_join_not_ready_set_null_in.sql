-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104130
-- nullIn / notNullIn on a not-ready set inside an ANY JOIN filter used to abort with
-- LOGICAL_ERROR "Not-ready Set is passed as the second argument for function 'nullIn'"
-- during plan-time dry-run in convertAnyJoinToSemiOrAntiJoin (STID 0250-4409).

DROP TABLE IF EXISTS t_left_04340;
DROP TABLE IF EXISTS t_right_04340;
DROP TABLE IF EXISTS t_subq_04340;

CREATE TABLE t_left_04340  (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_right_04340 (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_subq_04340  (x UInt64)           ENGINE = MergeTree ORDER BY x;

INSERT INTO t_left_04340  VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t_right_04340 VALUES (1, 100), (2, 200);
INSERT INTO t_subq_04340  VALUES (1);

-- enable_analyzer: the old analyzer disables convertAnyJoinToSemiOrAntiJoin.
-- query_plan_convert_any_join_to_semi_or_anti_join / *_filter_push_down etc. are
-- randomized by the runner; pin them so the filter stays above the ANY JOIN and the
-- optimization under test actually runs.
SET enable_analyzer = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;

-- Case 1: ANY LEFT JOIN, nullIn not-ready set. Filter is TRUE at runtime (1 is in the
-- subquery), so all rows pass: matched a=1,2 plus unmatched a=3.
SELECT a, b, c
FROM t_left_04340
ANY LEFT JOIN t_right_04340 USING (a)
WHERE toUInt64(nullIn(1, (SELECT x FROM t_subq_04340))) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0, query_plan_split_filter = 0, query_plan_merge_filters = 0;

-- Case 2: ANY RIGHT JOIN variant (JoinKind::Right code path).
SELECT a, b, c
FROM t_left_04340
ANY RIGHT JOIN t_right_04340 USING (a)
WHERE toUInt64(nullIn(1, (SELECT x FROM t_subq_04340))) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0;

-- Case 3: notNullIn variant. 1 notNullIn (1) = 0, so toUInt64(0) > 0 is FALSE: no rows.
SELECT a, b, c
FROM t_left_04340
ANY LEFT JOIN t_right_04340 USING (a)
WHERE toUInt64(notNullIn(1, (SELECT x FROM t_subq_04340))) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0;

-- Case 4: same not-ready-set nullIn inside the right branch of INTERSECT ALL, matching
-- the original fuzzer query shape (INTERSECT ALL generates the ANY JOIN). >= 0 keeps the
-- filter always TRUE, so the intersection is the left branch unchanged.
SELECT a, b FROM t_left_04340 WHERE a > 0
INTERSECT ALL
SELECT a, b
FROM t_left_04340
ANY LEFT JOIN t_right_04340 USING (a)
WHERE toUInt64(nullIn(a, (SELECT x FROM t_subq_04340))) >= 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0;

DROP TABLE t_left_04340;
DROP TABLE t_right_04340;
DROP TABLE t_subq_04340;
