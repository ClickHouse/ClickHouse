CREATE TABLE visits
(
    `CounterID` UInt32,
    `StartDate` Date,
    `StartTime` DateTime,
    `GoalsID` Array(UInt32),
    `Sign` Int8
)
ENGINE = Null;


CREATE MATERIALIZED VIEW goal_view TO goal
(
    `CounterID` UInt32,
    `StartDate` Date,
    `GoalID` UInt32,
    `Visits` AggregateFunction(sumIf, Int8, UInt8),
    `GoalReaches` AggregateFunction(sum, Int8)
) AS
SELECT
    CounterID,
    StartDate,
    GoalID,
    sumIfState(Sign, _uniq = 1) AS Visits,
    sumState(Sign) AS GoalReaches
FROM visits
ARRAY JOIN
    GoalsID AS GoalID,
    arrayEnumerateUniq(GoalsID) AS _uniq
GROUP BY
    CounterID,
    StartDate,
    GoalID
ORDER BY
    CounterID ASC,
    StartDate ASC,
    GoalID ASC;

CREATE TABLE goal
(
     `CounterID` UInt32,
     `StartDate` Date,
     `GoalID` UInt32,
     `Visits` AggregateFunction(sumIf, Int8, UInt8),
     `GoalReaches` AggregateFunction(sum, Int8)
) ENGINE = AggregatingMergeTree PARTITION BY toStartOfMonth(StartDate) ORDER BY (CounterID, StartDate, GoalID) SETTINGS index_granularity = 256;

INSERT INTO visits (`CounterID`,`StartDate`,`StartTime`,`Sign`,`GoalsID`) VALUES (1, toDate('2000-01-01'), toDateTime(toDate('2000-01-01')), 1, [1]);

DROP TABLE goal_view;
DROP TABLE goal;
DROP TABLE visits;
