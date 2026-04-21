-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102981
-- `filterResultForMatchedRows` in `tryConvertAnyJoinToSemiOrAntiJoin` must return UNKNOWN
-- (not FALSE) when the filter contains a `notIn(constant, subquery)` whose ColumnSet is
-- not yet built, otherwise the optimizer incorrectly converts ANY LEFT/RIGHT JOIN to ANTI
-- JOIN and drops matched rows from the result.
--
-- `query_plan_filter_push_down=0` prevents the filter from being pushed below the JOIN
-- before `tryConvertAnyJoinToSemiOrAntiJoin` runs, which is required to expose the bug.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;
DROP TABLE IF EXISTS t_s;

CREATE TABLE t_l (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_s (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_l VALUES (1), (2);
INSERT INTO t_r VALUES (1);
INSERT INTO t_s VALUES (42);

-- ANY LEFT JOIN: notIn(1, {42}) = true for every row; both (1, 1) and (2, 0) must appear.
SELECT t_l.id, t_r.id
FROM t_l ANY LEFT JOIN t_r ON t_l.id = t_r.id
WHERE notIn(1, (SELECT id FROM t_s))
ORDER BY t_l.id
SETTINGS query_plan_filter_push_down = 0;

DROP TABLE t_l;
DROP TABLE t_r;
DROP TABLE t_s;

-- ANY RIGHT JOIN: symmetric case — covers the JoinKind::Right branch of
-- tryConvertAnyJoinToSemiOrAntiJoin, which also calls filterResultForMatchedRows.
CREATE TABLE t_l (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_s (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_l VALUES (1);
INSERT INTO t_r VALUES (1), (2);
INSERT INTO t_s VALUES (42);

-- notIn(1, {42}) = true for every row; both (1, 1) and (0, 2) must appear.
-- Without the fix the optimizer would convert to ANTI RIGHT JOIN and drop row (1, 1).
SELECT t_l.id, t_r.id
FROM t_l ANY RIGHT JOIN t_r ON t_l.id = t_r.id
WHERE notIn(1, (SELECT id FROM t_s))
ORDER BY t_r.id
SETTINGS query_plan_filter_push_down = 0;

DROP TABLE t_l;
DROP TABLE t_r;
DROP TABLE t_s;
