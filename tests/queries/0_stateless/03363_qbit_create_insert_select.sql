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
INSERT INTO qbits_16 VALUES (4, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); -- { error TYPE_MISMATCH }


SELECT * FROM qbits_16 ORDER BY id;
DROP TABLE qbits_16;

-- 32-bit QBit
SELECT 'TEST 32-bit QBit';
DROP TABLE IF EXISTS qbits_32;
CREATE TABLE qbits_32 (id UInt32, vec QBit(Float32, 16)) ENGINE = Memory;

INSERT INTO qbits_32 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
INSERT INTO qbits_32 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_32 (id) VALUES (3);
INSERT INTO qbits_32 VALUES (4, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); -- { error TYPE_MISMATCH }

SELECT * FROM qbits_32 ORDER BY id;
DROP TABLE qbits_32;

-- 64-bit QBit
SELECT 'TEST 64-bit QBit';
DROP TABLE IF EXISTS qbits_64;
CREATE TABLE qbits_64 (id UInt32, vec QBit(Float64, 16)) ENGINE = Memory;

INSERT INTO qbits_64 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
INSERT INTO qbits_64 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_64 (id) VALUES (3);
INSERT INTO qbits_64 VALUES (4, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); -- { error TYPE_MISMATCH }

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
INSERT INTO qbits_16 VALUES (4, [0]); -- { error TYPE_MISMATCH }

SELECT * FROM qbits_16 ORDER BY id;
DROP TABLE qbits_16;

-- 32-bit QBit
SELECT 'TEST 32-bit QBit';
CREATE TABLE qbits_32 (id UInt32, vec QBit(Float32, 9)) ENGINE = Memory;

INSERT INTO qbits_32 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
INSERT INTO qbits_32 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_32 (id) VALUES (3);
INSERT INTO qbits_32 VALUES (4, [0]); -- { error TYPE_MISMATCH }

SELECT * FROM qbits_32 ORDER BY id;
DROP TABLE qbits_32;

-- 64-bit QBit
SELECT 'TEST 64-bit QBit';
CREATE TABLE qbits_64 (id UInt32, vec QBit(Float64, 9)) ENGINE = Memory;

INSERT INTO qbits_64 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
INSERT INTO qbits_64 VALUES (2, [0, 0, 0, 0, 0, 0, 0, 0, -32]);
INSERT INTO qbits_64 (id) VALUES (3);
INSERT INTO qbits_64 VALUES (4, [0]); -- { error TYPE_MISMATCH }

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

-- Difficult test
SELECT 'Difficult test';

DROP TABLE IF EXISTS array_64;
DROP TABLE IF EXISTS array_32;
DROP TABLE IF EXISTS array_16;
DROP TABLE IF EXISTS qbit_64;
DROP TABLE IF EXISTS qbit_32;
DROP TABLE IF EXISTS qbit_16;

CREATE TABLE array_64 (id UInt32, vec Array(Float64))  ENGINE = Memory;
CREATE TABLE array_32 (id UInt32, vec Array(Float32))  ENGINE = Memory;
CREATE TABLE array_16 (id UInt32, vec Array(BFloat16))  ENGINE = Memory;

INSERT INTO array_64 VALUES (1, [-9.91396531e-02, 4.71480675e-02, 2.32308246e-02, 8.30315799e-02, 6.67378604e-02, 2.18743533e-02, -8.63768607e-02, 2.43411604e-02, 2.19195038e-02, 1.24536417e-02, -1.97167601e-02, 8.65434185e-02, 1.87990386e-02, 2.78113149e-02, 4.55952510e-02, -1.80673841e-02, 1.49496756e-02, 4.65492159e-02, 2.61444114e-02, 6.46661744e-02, -4.14983965e-02, 8.17299914e-03, 6.44170940e-02, 3.95379104e-02, 5.79034053e-02, -7.23726954e-03, -1.11746430e-01, -3.06927301e-02]);
INSERT INTO array_32 SELECT id, vec FROM array_64 WHERE id = 1;
INSERT INTO array_16 SELECT id, vec FROM array_64 WHERE id = 1;

CREATE TABLE qbit_64 (id UInt32, vec QBit(Float64, 28)) ENGINE = Memory;
CREATE TABLE qbit_32 (id UInt32, vec QBit(Float32, 28)) ENGINE = Memory;
CREATE TABLE qbit_16 (id UInt32, vec QBit(BFloat16, 28)) ENGINE = Memory;

INSERT INTO qbit_64 SELECT id, CAST(vec AS QBit(Float64, 28)) FROM array_64;
INSERT INTO qbit_32 SELECT id, CAST(vec AS QBit(Float32, 28)) FROM array_32;
INSERT INTO qbit_16 SELECT id, CAST(vec AS QBit(BFloat16, 28)) FROM array_16;

SELECT * FROM qbit_64 ORDER BY id;
SELECT * FROM qbit_32 ORDER BY id;
SELECT * FROM qbit_16 ORDER BY id;

DROP TABLE array_64;
DROP TABLE array_32;
DROP TABLE array_16;
DROP TABLE qbit_64;
DROP TABLE qbit_32;
DROP TABLE qbit_16;
