DROP TABLE IF EXISTS test_degs_rads;


CREATE TABLE test_degs_rads (degrees Float64) ENGINE = Memory;


INSERT INTO test_degs_rads VALUES (-1);
INSERT INTO test_degs_rads VALUES (-180);
INSERT INTO test_degs_rads VALUES (-180.6);
INSERT INTO test_degs_rads VALUES (-360);
INSERT INTO test_degs_rads VALUES (0);
INSERT INTO test_degs_rads VALUES (1);
INSERT INTO test_degs_rads VALUES (180);
INSERT INTO test_degs_rads VALUES (180.5);
INSERT INTO test_degs_rads VALUES (360);


select DEGREES(RADIANS(degrees)) from test_degs_rads order by degrees;

DROP TABLE test_degs_rads;
