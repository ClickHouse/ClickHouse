DROP TABLE IF EXISTS data_02222;
CREATE TABLE data_02222 engine=MergeTree() ORDER BY dummy AS SELECT * FROM system.one;
-- { echoOn }
WITH
    (SELECT * FROM data_02222) AS bm1,
    (SELECT * FROM data_02222) AS bm2,
    (SELECT * FROM data_02222) AS bm3,
    (SELECT * FROM data_02222) AS bm4,
    (SELECT * FROM data_02222) AS bm5,
    (SELECT * FROM data_02222) AS bm6,
    (SELECT * FROM data_02222) AS bm7,
    (SELECT * FROM data_02222) AS bm8,
    (SELECT * FROM data_02222) AS bm9,
    (SELECT * FROM data_02222) AS bm10
SELECT bm1, bm2, bm3, bm4, bm5, bm6, bm7, bm8, bm9, bm10 FROM data_02222;
-- { echoOff }
DROP TABLE data_02222;
