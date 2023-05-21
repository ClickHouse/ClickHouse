DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (x String) ENGINE=Memory;
-- no commonalities between keys
INSERT INTO tbl1 VALUES ('a'), ('b'), ('c');
SELECT trainEntropyLearnedHash(x, 'id1') FROM tbl1;
SELECT entropyLearnedHash(x, 'id1') FROM tbl1;

SELECT trainEntropyLearnedHash(x, 1) FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- SELECT trainEntropyLearnedHash(x, NULL) FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT trainEntropyLearnedHash(1, 'id1') FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT entropyLearnedHash(x, 'non-existing id') FROM tbl1; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS tbl2;
-- x -- train data, y -- right chosen symbols
CREATE TABLE tbl2 (x String, y String) ENGINE=Memory;
-- with commonalities between keys
INSERT INTO tbl2 VALUES ('ada', 'aa'), ('bda', 'ba'), ('adb', 'ab');
SELECT trainEntropyLearnedHash(x, 'id2') FROM tbl2;
-- these two calls must have equal results
SELECT entropyLearnedHash(x, 'id2') FROM tbl2;
SELECT cityHash64(y) FROM tbl2;

DROP TABLE IF EXISTS tbl3;
CREATE TABLE tbl3 (x String, y String) ENGINE=Memory;
-- first position 3 will be chosen, and then 2. this test checks that the array of positions it sorted.
INSERT INTO tbl3 VALUES ('taa', 'aa'), ('tab', 'ab'), ('tac', 'ac'), ('tbc', 'bc');
SELECT trainEntropyLearnedHash(x, 'id3') FROM tbl3;
SELECT entropyLearnedHash(x, 'id3') FROM tbl3;
SELECT cityHash64(y) FROM tbl3;

DROP TABLE IF EXISTS tbl4;
CREATE TABLE tbl4 (x String, y String) ENGINE=Memory;
-- different length strings
INSERT INTO tbl4 VALUES ('a', 'a'), ('b', 'b'), ('acb', 'ab'), ('bca', 'ba');
SELECT trainEntropyLearnedHash(x, 'id4') FROM tbl4;
SELECT entropyLearnedHash(x, 'id4') FROM tbl4;
SELECT cityHash64(y) FROM tbl4;

DROP TABLE IF EXISTS tbl5;
CREATE TABLE tbl5 (x String) ENGINE=Memory;
INSERT INTO tbl5 VALUES ('a'), ('b'), ('c');
SELECT trainEntropyLearnedHash(x, 'id1') FROM tbl5;
SELECT entropyLearnedHash(x, 'id1', 'farmHash64') FROM tbl5;
SELECT farmHash64('a');
SELECT farmHash64('b');
SELECT farmHash64('c');

DROP TABLE tbl1;
DROP TABLE tbl2;
DROP TABLE tbl3;
DROP TABLE tbl4;
DROP TABLE tbl5;
