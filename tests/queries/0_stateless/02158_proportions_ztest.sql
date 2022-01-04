SELECT proportionsZTest(10, 11, 100, 101, 0.95, 'two-sided', 'unpooled');

DROP TABLE IF EXISTS proportions_ztest;
CREATE TABLE proportions_ztest (sx UInt64, sy UInt64, tx UInt64, ty UInt64, ci Float64, alt String, var String) Engine = Memory();
INSERT INTO proportions_ztest VALUES (10, 11, 100, 101, 0.95, 'two-sided', 'unpooled');
SELECT proportionsZTest(sx, sy, tx, ty, ci, alt, var) FROM proportions_ztest;
DROP TABLE IF EXISTS proportions_ztest;
