DROP TABLE IF EXISTS welch_ttest;
CREATE TABLE welch_ttest (left Float64, right Float64) ENGINE = Memory;
INSERT INTO welch_ttest VALUES (27.5,27.1), (21.0,22.0), (19.0,20.8), (23.6,23.4), (17.0,23.4), (17.9,23.5), (16.9,25.8), (20.1,22.0), (21.9,24.8), (22.6,20.2), (23.1,21.9), (19.6,22.1), (19.0,22.9), (21.7,20.5), (21.4,24.4);
SELECT '0.021378001462867';
SELECT roundBankers(WelchTTest(left, right), 8) from welch_ttest;
DROP TABLE IF EXISTS welch_ttest;

CREATE TABLE welch_ttest (left Float64, right Float64) ENGINE = Memory;
INSERT INTO welch_ttest VALUES (30.02,29.89), (29.99,29.93), (30.11,29.72), (29.97,29.98), (30.01,30.02), (29.99,29.98);
SELECT '0.090773324285671';
SELECT roundBankers(WelchTTest(left, right), 8) from welch_ttest;
DROP TABLE IF EXISTS welch_ttest;

CREATE TABLE welch_ttest (left Float64, right Float64) ENGINE = Memory;
INSERT INTO welch_ttest VALUES (0.010268,0.159258), (0.000167,0.136278), (0.000167,0.122389);
SELECT '0.00339907162713746';
SELECT roundBankers(WelchTTest(left, right), 8) from welch_ttest;
DROP TABLE IF EXISTS welch_ttest;