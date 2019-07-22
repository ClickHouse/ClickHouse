DROP TABLE IF EXISTS limit_by;
CREATE TABLE limit_by (Num UInt32, Name String) ENGINE = Memory;

INSERT INTO limit_by (Num, Name) VALUES (1, 'John');
INSERT INTO limit_by (Num, Name) VALUES (1, 'John');
INSERT INTO limit_by (Num, Name) VALUES (3, 'Mary');
INSERT INTO limit_by (Num, Name) VALUES (3, 'Mary');
INSERT INTO limit_by (Num, Name) VALUES (3, 'Mary');
INSERT INTO limit_by (Num, Name) VALUES (4, 'Mary');
INSERT INTO limit_by (Num, Name) VALUES (4, 'Mary');
INSERT INTO limit_by (Num, Name) VALUES (5, 'Bill');
INSERT INTO limit_by (Num, Name) VALUES (7, 'Bill');
INSERT INTO limit_by (Num, Name) VALUES (7, 'Bill');
INSERT INTO limit_by (Num, Name) VALUES (7, 'Mary');
INSERT INTO limit_by (Num, Name) VALUES (7, 'John');

-- Two elemens in each group
SELECT Num FROM limit_by ORDER BY Num LIMIT 2 BY Num;

-- LIMIT BY doesn't affect result of GROUP BY
SELECT Num, count(*) FROM limit_by GROUP BY Num ORDER BY Num LIMIT 2 BY Num;

-- LIMIT BY can be combined with LIMIT
SELECT Num, Name FROM limit_by ORDER BY Num LIMIT 1 BY Num, Name LIMIT 3;

-- Distributed LIMIT BY
SELECT dummy FROM remote('127.0.0.{2,3}', system.one) LIMIT 1 BY dummy;
SELECT dummy FROM remote('127.0.0.{2,3}', system.one) LIMIT 2 BY dummy;

SELECT 1 as one FROM remote('127.0.0.{2,3}', system.one) LIMIT 1 BY one;

-- Distributed LIMIT BY with LIMIT
SELECT toInt8(number / 5 + 100) AS x FROM remote('127.0.0.1', system.numbers) LIMIT 2 BY x LIMIT 5;

-- Distributed LIMIT BY with ORDER BY non-selected column
SELECT 1 AS x FROM remote('127.0.0.{2,3}', system.one) ORDER BY dummy LIMIT 1 BY x;

DROP TABLE IF EXISTS limit_by;
