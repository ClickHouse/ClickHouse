-- Tags: stateful
DROP TABLE IF EXISTS basic_00040;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE MATERIALIZED VIEW basic_00040
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
FROM basic_00040
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sumMerge(Visits)	AS Visits,
    uniqMerge(Users)	AS Users
FROM basic_00040
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sum(Sign) 			AS Visits,
    uniq(UserID)		AS Users
FROM test.visits
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


DROP TABLE basic_00040;
