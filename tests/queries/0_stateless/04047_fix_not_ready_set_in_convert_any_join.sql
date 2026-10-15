-- Regression test: evaluatePartialResult during convertAnyJoinToSemiOrAntiJoin
-- must not throw LOGICAL_ERROR when an IN function references a not-ready Set.
-- https://github.com/ClickHouse/ClickHouse/pull/99939

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (1, 100), (2, 200), (4, 400);

-- The filter mixes a join-dependent column (c) with a constant IN subquery.
-- Disabling filter pushdown and splitting keeps the filter above the JoinStepLogical,
-- so convertAnyJoinToSemiOrAntiJoin runs evaluatePartialResult on a DAG
-- containing an IN function whose Set is not yet ready.
SELECT a, b, c
FROM t1
ANY LEFT JOIN t2 USING (a)
WHERE c + toUInt64(1 IN (SELECT number FROM numbers(10))) > 0
ORDER BY a
SETTINGS query_plan_filter_push_down = 0, query_plan_split_filter = 0, query_plan_merge_filters = 0;

DROP TABLE t1;
DROP TABLE t2;
