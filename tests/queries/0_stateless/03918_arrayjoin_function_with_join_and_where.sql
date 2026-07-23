-- Tags: no-random-settings

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/96398
-- arrayJoin() function with INNER JOIN and WHERE should not produce duplicate rows.
-- The bug was in partial predicate push-down for JOINs (disjunction optimization):
-- it didn't check for ARRAY_JOIN nodes, causing arrayJoin to execute both
-- before and after the JOIN.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt32, arr Array(UInt32)) ENGINE = Memory;
CREATE TABLE t2 (a UInt32) ENGINE = Memory;

INSERT INTO t1 VALUES (1, [10, 20]), (2, [30]);
INSERT INTO t2 VALUES (1), (2);

-- Simple case: arrayJoin with INNER JOIN and WHERE
SELECT a, arrayJoin(arr) AS x
FROM t1
INNER JOIN t2 USING (a)
WHERE x > 0
ORDER BY a, x;

-- Each (a, x) pair should appear exactly once
SELECT count()
FROM (
    SELECT a, arrayJoin(arr) AS x
    FROM t1
    INNER JOIN t2 USING (a)
    WHERE x > 0
);

DROP TABLE t1;
DROP TABLE t2;
