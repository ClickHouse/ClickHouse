-- test conversion from degrees to radians
DROP TABLE IF EXISTS test_degs_to_rads;

CREATE TABLE test_degs_to_rads (degrees Float64) ENGINE = Memory;

INSERT INTO test_degs_to_rads VALUES (-1);
INSERT INTO test_degs_to_rads VALUES (-180);
INSERT INTO test_degs_to_rads VALUES (-180.6);
INSERT INTO test_degs_to_rads VALUES (-360);
INSERT INTO test_degs_to_rads VALUES (0);
INSERT INTO test_degs_to_rads VALUES (1);
INSERT INTO test_degs_to_rads VALUES (180);
INSERT INTO test_degs_to_rads VALUES (180.5);
INSERT INTO test_degs_to_rads VALUES (360);

-- test that converting degrees to radians and back preserves the original value
select DEGREES(RADIANS(degrees)) from test_degs_to_rads order by degrees;
-- test that radians func returns correct value for both int and floats
select RADIANS(degrees) from test_degs_to_rads order by degrees;

DROP TABLE test_degs_to_rads;

-- test conversion from radians to degrees
DROP TABLE IF EXISTS test_rads_to_degs;

CREATE TABLE test_rads_to_degs (radians Float64) ENGINE = Memory;

INSERT INTO test_rads_to_degs VALUES (-6.283185307179586);
INSERT INTO test_rads_to_degs VALUES (-3.152064629101759);
INSERT INTO test_rads_to_degs VALUES (-3.141592653589793);
INSERT INTO test_rads_to_degs VALUES (-0.017453292519943295);
INSERT INTO test_rads_to_degs VALUES(0);
INSERT INTO test_rads_to_degs VALUES(1);
INSERT INTO test_rads_to_degs VALUES(10);
INSERT INTO test_rads_to_degs VALUES(-10);
INSERT INTO test_rads_to_degs VALUES (0.017453292519943295);
INSERT INTO test_rads_to_degs VALUES (3.141592653589793);
INSERT INTO test_rads_to_degs VALUES (3.1503192998497647);
INSERT INTO test_rads_to_degs VALUES (6.283185307179586);

-- test that converting radians to degrees and back preserves the original value
select RADIANS(DEGREES(radians)) from test_rads_to_degs order by radians;
-- test that degrees func returns correct value for both int and floats
select DEGREES(radians) from test_rads_to_degs order by radians;

DROP TABLE test_rads_to_degs;
