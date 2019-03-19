DROP TABLE IF EXISTS test.limit_by;
CREATE TABLE test.limit_by (Num UInt32, Name String) ENGINE = Memory;

INSERT INTO test.limit_by (Num, Name) VALUES (1, 'John');
INSERT INTO test.limit_by (Num, Name) VALUES (1, 'John');
INSERT INTO test.limit_by (Num, Name) VALUES (3, 'Mary');
INSERT INTO test.limit_by (Num, Name) VALUES (3, 'Mary');
INSERT INTO test.limit_by (Num, Name) VALUES (3, 'Mary');
INSERT INTO test.limit_by (Num, Name) VALUES (4, 'Mary');
INSERT INTO test.limit_by (Num, Name) VALUES (4, 'Mary');
INSERT INTO test.limit_by (Num, Name) VALUES (5, 'Bill');
INSERT INTO test.limit_by (Num, Name) VALUES (7, 'Bill');
INSERT INTO test.limit_by (Num, Name) VALUES (7, 'Bill');
INSERT INTO test.limit_by (Num, Name) VALUES (7, 'Mary');
INSERT INTO test.limit_by (Num, Name) VALUES (7, 'John');

-- Two elemens in each group
SELECT Num FROM test.limit_by ORDER BY Num LIMIT 2 BY Num;

-- LIMIT BY doesn't affect result of GROUP BY
SELECT Num, count(*) FROM test.limit_by GROUP BY Num ORDER BY Num LIMIT 2 BY Num;

-- LIMIT BY can be combined with LIMIT
SELECT Num, Name FROM test.limit_by ORDER BY Num LIMIT 1 BY Num, Name LIMIT 3;

-- Distributed LIMIT BY
SELECT dummy FROM remote('127.0.0.{2,3}', system.one) LIMIT 1 BY dummy;
SELECT dummy FROM remote('127.0.0.{2,3}', system.one) LIMIT 2 BY dummy;

SELECT 1 as one FROM remote('127.0.0.{2,3}', system.one) LIMIT 1 BY one;

-- Distributed LIMIT BY with LIMIT
SELECT toInt8(number / 5 + 100) AS x FROM remote('127.0.0.1', system.numbers) LIMIT 2 BY x LIMIT 5;

DROP TABLE IF EXISTS test.limit_by;
