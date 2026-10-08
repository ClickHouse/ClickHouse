-- Tags: stateful
DROP TABLE IF EXISTS basic;
DROP TABLE IF EXISTS visits_null;

CREATE TABLE visits_null
(
    CounterID UInt32,
    StartDate Date,
    Sign Int8,
    UserID UInt64
) ENGINE = Null;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE MATERIALIZED VIEW basic
ENGINE = AggregatingMergeTree(StartDate, (CounterID, StartDate), 8192)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)                  AS Visits,
    uniqState(UserID)               AS Users
FROM visits_null
GROUP BY CounterID, StartDate;

INSERT INTO visits_null
SELECT
    CounterID,
    StartDate,
    Sign,
    UserID
FROM test.visits;


SELECT
    StartDate,
    sumMerge(Visits)                AS Visits,
    uniqMerge(Users)                AS Users
FROM basic
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sumMerge(Visits)                AS Visits,
    uniqMerge(Users)                AS Users
FROM basic
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sum(Sign)                       AS Visits,
    uniq(UserID)                    AS Users
FROM test.visits
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


OPTIMIZE TABLE basic;
OPTIMIZE TABLE basic;
OPTIMIZE TABLE basic;
OPTIMIZE TABLE basic;
OPTIMIZE TABLE basic;
OPTIMIZE TABLE basic;
OPTIMIZE TABLE basic;
OPTIMIZE TABLE basic;
OPTIMIZE TABLE basic;
OPTIMIZE TABLE basic;


DROP TABLE visits_null;
DROP TABLE basic;
