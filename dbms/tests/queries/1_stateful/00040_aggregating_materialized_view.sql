DROP TABLE IF EXISTS test.basic;

CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree(StartDate, (CounterID, StartDate), 8192)
POPULATE AS
SELECT
    CounterID,
    StartDate,
    sumState(Sign) 		AS Visits,
    uniqState(UserID)	AS Users
FROM test.visits
GROUP BY CounterID, StartDate;


SELECT
    StartDate,
    sumMerge(Visits)	AS Visits,
    uniqMerge(Users)	AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sumMerge(Visits)	AS Visits,
    uniqMerge(Users)	AS Users
FROM test.basic
WHERE CounterID = 731962
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sum(Sign) 			AS Visits,
    uniq(UserID)		AS Users
FROM test.visits
WHERE CounterID = 731962
GROUP BY StartDate
ORDER BY StartDate;


DROP TABLE test.basic;
