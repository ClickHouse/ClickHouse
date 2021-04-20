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

DROP TABLE IF EXISTS mv_src;
CREATE TABLE mv_src ( `UserID` UInt32, `Region` String, `Day` UInt8, `Age` UInt8, `Sex` UInt8 ) ENGINE = MergeTree() ORDER BY (Day, UserID);

DROP TABLE IF EXISTS mv;
CREATE MATERIALIZED VIEW mv
ENGINE = AggregatingMemory()
AS SELECT
    Region,
    Day,
    avg(Age) AS AvgAge,
    count() AS Visits,
    countIf(Sex = 1) AS Boys,
    uniq(UserID) AS Users
FROM mv_src
GROUP BY Region, Day;

INSERT INTO mv_src SELECT * FROM visits_data WHERE Sex = 2;
INSERT INTO mv_src SELECT * FROM visits_data WHERE Sex != 2;

SELECT * FROM mv;

DROP TABLE IF EXISTS mv;