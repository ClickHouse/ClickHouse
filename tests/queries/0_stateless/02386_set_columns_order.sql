DROP TABLE IF EXISTS userid_set;
DROP TABLE IF EXISTS userid_test;
DROP TABLE IF EXISTS userid_set2;

CREATE TABLE userid_set(userid UInt64, name String) ENGINE = Set;
INSERT INTO userid_set VALUES (1, 'Mary'),(2, 'Jane'),(3, 'Mary'),(4, 'Jack');

CREATE TABLE userid_test (userid UInt64, name String) ENGINE = MergeTree() PARTITION BY (intDiv(userid, 500)) ORDER BY (userid) SETTINGS index_granularity = 8192;
INSERT INTO userid_test VALUES (1, 'Jack'),(2, 'Mary'),(3, 'Mary'),(4, 'John'),(5, 'Mary');

SELECT * FROM userid_test WHERE (userid, name) IN (userid_set);

CREATE TABLE userid_set2(userid UInt64, name String, birthdate Date) ENGINE = Set;
INSERT INTO userid_set2 values (1,'John', '1990-01-01');

WITH  'John' AS name,  toDate('1990-01-01') AS birthdate
SELECT * FROM numbers(10)
WHERE (number, name, birthdate) IN (userid_set2);

DROP TABLE userid_set;
DROP TABLE userid_test;
DROP TABLE userid_set2;
