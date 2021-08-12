DROP TABLE IF EXISTS aggr_memory;
DROP TABLE IF EXISTS visits_data;

CREATE TABLE visits_data ( `UserID` UInt32, `Region` String, `Day` UInt8, `Age` UInt8, `Sex` UInt8 ) ENGINE = MergeTree() ORDER BY (Day, UserID);

INSERT INTO visits_data VALUES (1, 'Moscow', 1, 30, 2);
INSERT INTO visits_data VALUES (2, 'SPb', 1, 32, 2);
INSERT INTO visits_data VALUES (3, 'Moscow', 1, 22, 1);
INSERT INTO visits_data VALUES (4, 'Moscow', 1, 27, 2);
INSERT INTO visits_data VALUES (5, 'SPb', 1, 40, 0);
INSERT INTO visits_data VALUES (6, 'SPb', 1, 20, 2);
INSERT INTO visits_data VALUES (3, 'Moscow', 2, 22, 1);
INSERT INTO visits_data VALUES (5, 'SPb', 2, 40, 0);
INSERT INTO visits_data VALUES (2, 'SPb', 2, 32, 2);
INSERT INTO visits_data VALUES (1, 'Moscow', 2, 30, 2);
INSERT INTO visits_data VALUES (3, 'Moscow', 2, 22, 1);


CREATE TABLE aggr_memory ENGINE = AggregatingMemory()
AS SELECT
    Region,
    Day,
    avg(Age) AS AvgAge,
    count() AS Visits,
    countIf(Sex = 1) AS Boys,
    uniq(UserID) AS Users
FROM visits_data
GROUP BY Region, Day
;

SELECT * FROM aggr_memory;

INSERT INTO aggr_memory SELECT * FROM visits_data WHERE Sex = 1;
SELECT * FROM aggr_memory;

INSERT INTO aggr_memory SELECT * FROM visits_data WHERE Sex != 1;
SELECT * FROM aggr_memory;

INSERT INTO aggr_memory SELECT * FROM visits_data WHERE Sex == 3;
SELECT * FROM aggr_memory;

DROP TABLE IF EXISTS aggr_memory;
DROP TABLE IF EXISTS visits_data;
