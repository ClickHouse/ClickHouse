DROP TABLE IF EXISTS empty_summing;
CREATE TABLE empty_summing (d Date, k UInt64, v Int8) ENGINE=SummingMergeTree(d, k, 8192);

INSERT INTO empty_summing VALUES ('2015-01-01', 1, 10);
INSERT INTO empty_summing VALUES ('2015-01-01', 1, -10);

OPTIMIZE TABLE empty_summing;
SELECT * FROM empty_summing;

INSERT INTO empty_summing VALUES ('2015-01-01', 1, 4),('2015-01-01', 2, -9),('2015-01-01', 3, -14);
INSERT INTO empty_summing VALUES ('2015-01-01', 1, -2),('2015-01-01', 1, -2),('2015-01-01', 3, 14);
INSERT INTO empty_summing VALUES ('2015-01-01', 1, 0),('2015-01-01', 3, 0);

OPTIMIZE TABLE empty_summing;
SELECT * FROM empty_summing;

DROP TABLE empty_summing;
