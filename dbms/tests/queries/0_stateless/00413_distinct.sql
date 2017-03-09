DROP TABLE IF EXISTS test.distinct;
CREATE TABLE test.distinct (Num UInt32, Name String) ENGINE = Memory;

INSERT INTO test.distinct (Num, Name) VALUES (1, 'John');
INSERT INTO test.distinct (Num, Name) VALUES (1, 'John');
INSERT INTO test.distinct (Num, Name) VALUES (3, 'Mary');
INSERT INTO test.distinct (Num, Name) VALUES (3, 'Mary');
INSERT INTO test.distinct (Num, Name) VALUES (3, 'Mary');
INSERT INTO test.distinct (Num, Name) VALUES (4, 'Mary');
INSERT INTO test.distinct (Num, Name) VALUES (4, 'Mary');
INSERT INTO test.distinct (Num, Name) VALUES (5, 'Bill');
INSERT INTO test.distinct (Num, Name) VALUES (7, 'Bill');
INSERT INTO test.distinct (Num, Name) VALUES (7, 'Bill');
INSERT INTO test.distinct (Num, Name) VALUES (7, 'Mary');
INSERT INTO test.distinct (Num, Name) VALUES (7, 'John');

-- String field
SELECT Name FROM (SELECT DISTINCT Name FROM test.distinct) ORDER BY Name;
-- Num field
SELECT Num FROM (SELECT DISTINCT Num FROM test.distinct) ORDER BY Num;

DROP TABLE IF EXISTS test.distinct;
