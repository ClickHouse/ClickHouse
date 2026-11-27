-- Tests for the Int8 element type of the QBit data type

SELECT 'TEST Int8 QBit, dimension % 8 == 0';
DROP TABLE IF EXISTS qbits_8;
CREATE TABLE qbits_8 (id UInt32, vec QBit(Int8, 16)) ENGINE = Memory;

INSERT INTO qbits_8 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
INSERT INTO qbits_8 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_8 VALUES (3, [-128, 127, -1, 1, -64, 64, -100, 100, -2, 2, -3, 3, -4, 4, -5, 5]);
INSERT INTO qbits_8 (id) VALUES (4);
INSERT INTO qbits_8 VALUES (5, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); -- { error SIZES_OF_ARRAYS_DONT_MATCH }

SELECT * FROM qbits_8 ORDER BY id;
DROP TABLE qbits_8;

SELECT 'TEST Int8 QBit, dimension % 8 != 0';
CREATE TABLE qbits_8 (id UInt32, vec QBit(Int8, 9)) ENGINE = Memory;

INSERT INTO qbits_8 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
INSERT INTO qbits_8 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_8 (id) VALUES (3);
INSERT INTO qbits_8 VALUES (4, [0]); -- { error SIZES_OF_ARRAYS_DONT_MATCH }

SELECT * FROM qbits_8 ORDER BY id;
DROP TABLE qbits_8;

SELECT 'TEST Int8 QBit subcolumns (bit planes, MSB first)';
CREATE TABLE qbits_8 (id UInt32, vec QBit(Int8, 8)) ENGINE = Memory;
INSERT INTO qbits_8 VALUES (1, [0, 0, 0, 0, 0, 0, 0, 0]);
INSERT INTO qbits_8 VALUES (2, [-1, -1, -1, -1, -1, -1, -1, -1]);
-- The first subcolumn is the sign bit plane: 0 for non-negative, 1 for negative values
SELECT id, bin(vec.1) FROM qbits_8 ORDER BY id;
DROP TABLE qbits_8;

SELECT 'TEST INSERTS THROUGH convertFieldToType';
CREATE TABLE qbits_8 (id UInt32, vec QBit(Int8, 1)) ENGINE = Memory;
INSERT INTO qbits_8 VALUES (1, [toInt8(5)]);
INSERT INTO qbits_8 VALUES (2, [toFloat64(7)]);
INSERT INTO qbits_8 VALUES (3, [-9]);
SELECT * FROM qbits_8 ORDER BY id;
DROP TABLE qbits_8;

SELECT 'TEST CAST between Array and Int8 QBit';
DROP TABLE IF EXISTS array_8;
DROP TABLE IF EXISTS qbit_8;
CREATE TABLE array_8 (id UInt32, vec Array(Int8)) ENGINE = Memory;
CREATE TABLE qbit_8 (id UInt32, vec QBit(Int8, 12)) ENGINE = Memory;

INSERT INTO array_8 VALUES (1, [-128, -100, -50, -10, -1, 0, 1, 10, 50, 100, 126, 127]);
INSERT INTO qbit_8 SELECT id, CAST(vec AS QBit(Int8, 12)) FROM array_8;

SELECT vec FROM qbit_8 ORDER BY id;

DROP TABLE array_8;
DROP TABLE qbit_8;

SELECT 'TEST out-of-range values wrap to Int8, consistently with Int8 / Array(Int8)';
-- Reference: toInt8 wraps (128 -> -128, -129 -> 127, 256 -> 0, 300 -> 44)
SELECT toInt8(128), toInt8(-129), toInt8(256), toInt8(300);
SELECT CAST([128, -129, 256, 300] AS Array(Int8));
-- QBit(Int8) wraps the same way through VALUES
CREATE TABLE qbits_8 (vec QBit(Int8, 4)) ENGINE = Memory;
INSERT INTO qbits_8 VALUES ([128, -129, 256, 300]);
SELECT vec FROM qbits_8;
DROP TABLE qbits_8;
-- ... and through CAST
SELECT CAST([128, -129, 256, 300] AS QBit(Int8, 4));

SELECT 'TEST non-finite floats are rejected (consistent with Array(Int8))';
SELECT CAST([nan] AS Array(Int8)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST([nan] AS QBit(Int8, 1)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST([inf] AS QBit(Int8, 1)); -- { serverError CANNOT_CONVERT_TYPE }
