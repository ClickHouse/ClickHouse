DROP TABLE IF EXISTS VisitsData;
DROP TABLE IF EXISTS AggrMemory;

CREATE TABLE VisitsData ( `UserID` UInt32, `Region` String, `Day` UInt8, `Age` UInt8, `Sex` UInt8 ) ENGINE = MergeTree() ORDER BY (Day, UserID);

INSERT INTO VisitsData VALUES (1, 'Moscow', 1, 30, 2);
INSERT INTO VisitsData VALUES (2, 'SPb', 1, 32, 2);
INSERT INTO VisitsData VALUES (3, 'Moscow', 1, 22, 1);
INSERT INTO VisitsData VALUES (4, 'Moscow', 1, 27, 2);
INSERT INTO VisitsData VALUES (5, 'SPb', 1, 40, 0);
INSERT INTO VisitsData VALUES (6, 'SPb', 1, 20, 2);
INSERT INTO VisitsData VALUES (3, 'Moscow', 2, 22, 1);
INSERT INTO VisitsData VALUES (5, 'SPb', 2, 40, 0);
INSERT INTO VisitsData VALUES (2, 'SPb', 2, 32, 2);
INSERT INTO VisitsData VALUES (1, 'Moscow', 2, 30, 2);
INSERT INTO VisitsData VALUES (3, 'Moscow', 2, 22, 1);


CREATE TABLE AggrMemory
( `UserID` UInt32, `Region` String, `Day` UInt8, `Age` UInt8, `Sex` UInt8 ) 
ENGINE = AggregatingMemory()
AS SELECT
    Region,
    Day,
    avg(Age) AS AvgAge,
    count() AS Visits,
    countIf(Sex = 1) AS Boys,
    uniq(UserID) AS Users
FROM VisitsData
GROUP BY Region, Day
;

INSERT INTO AggrMemory SELECT * FROM test.sample WHERE Sex = 2;
INSERT INTO AggrMemory SELECT * FROM test.sample WHERE Sex != 2;

SELECT * FROM AggrMemory;

DROP TABLE IF EXISTS Visits;
DROP TABLE IF EXISTS AggrMemory;
