SET allow_experimental_qbit_type = 1;

DROP TABLE IF EXISTS qbits;

SELECT 'Test QBit with MergeTree engine';
CREATE TABLE qbits (id UInt32, vec QBit(BFloat16, 16)) ENGINE = MergeTree ORDER BY id;

INSERT INTO qbits VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, -16]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);

-- Check everything is correct after parts are merged
SELECT * FROM qbits ORDER BY id;
OPTIMIZE TABLE qbits FINAL;
SELECT * FROM qbits ORDER BY id;

DROP TABLE qbits;


SELECT 'Test QBit with ReplacingMergeTree engine';
CREATE TABLE qbits (id UInt32, vec QBit(BFloat16, 16)) ENGINE = ReplacingMergeTree ORDER BY id;

-- Add duplicates to test replacing
INSERT INTO qbits VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, -16]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);

SELECT * FROM qbits ORDER BY id;
OPTIMIZE TABLE qbits FINAL;
SELECT * FROM qbits ORDER BY id;

DROP TABLE qbits;


SELECT 'Test QBit with CoalescingMergeTree engine';
CREATE TABLE qbits (id UInt32, vec QBit(BFloat16, 16)) ENGINE = CoalescingMergeTree ORDER BY id;

INSERT INTO qbits VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, -16]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);
INSERT INTO qbits VALUES (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, -32]);

SELECT * FROM qbits ORDER BY id;
OPTIMIZE TABLE qbits FINAL;
SELECT * FROM qbits ORDER BY id;

DROP TABLE qbits;
