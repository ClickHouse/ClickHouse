SELECT proportionsZTest(10, 11, 100, 101, 0.95, 'unpooled');

DROP TABLE IF EXISTS proportions_ztest;
CREATE TABLE proportions_ztest (sx UInt64, sy UInt64, tx UInt64, ty UInt64) Engine = Memory();
INSERT INTO proportions_ztest VALUES (10, 11, 100, 101);
SELECT proportionsZTest(sx, sy, tx, ty, 0.95, 'unpooled') FROM proportions_ztest;
DROP TABLE IF EXISTS proportions_ztest;


SELECT
    NULL,
    proportionsZTest(257, 1048575, 1048575, 257, -inf, NULL),
    proportionsZTest(1024, 1025, 2, 2, 'unpooled'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }