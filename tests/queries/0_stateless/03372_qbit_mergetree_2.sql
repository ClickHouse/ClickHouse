SET allow_experimental_qbit_type = 1;

DROP TABLE IF EXISTS qbits;

SELECT 'Test QBit with SummingMergeTree engine';
CREATE TABLE qbits (id UInt32, vec QBit(BFloat16, 16)) ENGINE = SummingMergeTree ORDER BY id;

-- The elements of qbits will not be summed and this is expected behavior
INSERT INTO qbits VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, -16]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);

SELECT * FROM qbits ORDER BY id;
OPTIMIZE TABLE qbits FINAL;
SELECT * FROM qbits ORDER BY id;

DROP TABLE qbits;


SELECT 'Test QBit with AggregatingMergeTree engine';
CREATE TABLE qbits (id UInt32, vec QBit(BFloat16, 16)) ENGINE = AggregatingMergeTree ORDER BY id;

INSERT INTO qbits VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, -16]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);

SELECT * FROM qbits ORDER BY id;
OPTIMIZE TABLE qbits FINAL;
SELECT * FROM qbits ORDER BY id;

DROP TABLE qbits;


SELECT 'Test QBit with CollapsingMergeTree engine';
CREATE TABLE qbits (id UInt8, sign Int8, vec QBit(BFloat16, 16), order UInt8) ENGINE = CollapsingMergeTree(sign) ORDER BY id;

INSERT INTO qbits VALUES (1, 1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, -16], 1);
INSERT INTO qbits VALUES (1, -1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, -16], 2);
INSERT INTO qbits VALUES (1, 1, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32], 3);

SELECT * FROM qbits ORDER BY order;
OPTIMIZE TABLE qbits FINAL;
SELECT * FROM qbits ORDER BY order;

DROP TABLE qbits;


SELECT 'Test QBit with VersionedCollapsingMergeTree engine';
CREATE TABLE qbits (id Int8, sign Int8, ver Int8, vec QBit(BFloat16, 16)) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY ver;

INSERT INTO qbits VALUES (0, 1, 0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, -16]);
INSERT INTO qbits VALUES (1, 1, 1, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);
INSERT INTO qbits VALUES (2, -1, 1, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);

SELECT * FROM qbits ORDER BY id;
OPTIMIZE TABLE qbits FINAL;
SELECT * FROM qbits ORDER BY id;

DROP TABLE qbits;
