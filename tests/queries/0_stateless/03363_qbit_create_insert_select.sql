SET allow_experimental_qbit_type = 1;

-- Tests when number of elements in QBit % 8 == 0
-- Also tests the default value of QBit when no default is specified (when inserting data without `vec`)
SELECT 'Tests when number of elements in QBit % 8 == 0';

-- 16-bit QBit
SELECT 'TEST 16-bit QBit';
DROP TABLE IF EXISTS qbits_16;
CREATE TABLE qbits_16 (id UInt32, vec QBit(BFloat16, 16)) ENGINE = Memory;

INSERT INTO qbits_16 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
INSERT INTO qbits_16 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_16 (id) VALUES (3);
INSERT INTO qbits_16 VALUES (4, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); -- { clientError TYPE_MISMATCH }


SELECT * FROM qbits_16 ORDER BY id;
DROP TABLE qbits_16;

-- 32-bit QBit
SELECT 'TEST 32-bit QBit';
DROP TABLE IF EXISTS qbits_32;
CREATE TABLE qbits_32 (id UInt32, vec QBit(Float32, 16)) ENGINE = Memory;

INSERT INTO qbits_32 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
INSERT INTO qbits_32 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_32 (id) VALUES (3);
INSERT INTO qbits_32 VALUES (4, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); -- { clientError TYPE_MISMATCH }

SELECT * FROM qbits_32 ORDER BY id;
DROP TABLE qbits_32;

-- 64-bit QBit
SELECT 'TEST 64-bit QBit';
DROP TABLE IF EXISTS qbits_64;
CREATE TABLE qbits_64 (id UInt32, vec QBit(Float64, 16)) ENGINE = Memory;

INSERT INTO qbits_64 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
INSERT INTO qbits_64 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_64 (id) VALUES (3);
INSERT INTO qbits_64 VALUES (4, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); -- { clientError TYPE_MISMATCH }

SELECT * FROM qbits_64 ORDER BY id;
DROP TABLE qbits_64;



-- Tests when number of elements in QBit % 8 != 0
SELECT 'Tests when number of elements in QBit % 8 == 1';

-- 16-bit QBit
SELECT 'TEST 16-bit QBit';
CREATE TABLE qbits_16 (id UInt32, vec QBit(BFloat16, 9)) ENGINE = Memory;

INSERT INTO qbits_16 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
INSERT INTO qbits_16 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_16 (id) VALUES (3);
INSERT INTO qbits_16 VALUES (4, [0]); -- { clientError TYPE_MISMATCH }

SELECT * FROM qbits_16 ORDER BY id;
DROP TABLE qbits_16;

-- 32-bit QBit
SELECT 'TEST 32-bit QBit';
CREATE TABLE qbits_32 (id UInt32, vec QBit(Float32, 9)) ENGINE = Memory;

INSERT INTO qbits_32 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
INSERT INTO qbits_32 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_32 (id) VALUES (3);
INSERT INTO qbits_32 VALUES (4, [0]); -- { clientError TYPE_MISMATCH }

SELECT * FROM qbits_32 ORDER BY id;
DROP TABLE qbits_32;

-- 64-bit QBit
SELECT 'TEST 64-bit QBit';
CREATE TABLE qbits_64 (id UInt32, vec QBit(Float64, 9)) ENGINE = Memory;

INSERT INTO qbits_64 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
INSERT INTO qbits_64 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_64 (id) VALUES (3);
INSERT INTO qbits_64 VALUES (4, [0]); -- { clientError TYPE_MISMATCH }

SELECT * FROM qbits_64 ORDER BY id;
DROP TABLE qbits_64;

SELECT 'TEST INSERTS THROUGH convertFieldToType';
CREATE TABLE qbits (id UInt32, vec QBit(BFloat16, 1)) ENGINE = Memory;
INSERT INTO qbits VALUES (1, [toFloat64(1)]);
SELECT * FROM qbits;
DROP TABLE qbits;

CREATE TABLE qbits (id UInt32, vec QBit(Float64, 1)) ENGINE = Memory;
INSERT INTO qbits VALUES (1, [toBFloat16(1)]);
SELECT * FROM qbits;
DROP TABLE qbits;
