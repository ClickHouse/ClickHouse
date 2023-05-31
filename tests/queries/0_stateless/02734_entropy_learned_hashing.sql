-- Tags: no-parallel
-- no-parallel because entropy-learned hash uses global state

SET allow_experimental_hash_functions = 1;

-- no commonalities between keys
DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (x String) ENGINE=Memory;
INSERT INTO tbl1 VALUES ('a'), ('b'), ('c');
SELECT prepareTrainEntropyLearnedHash(x, 'id1') FROM tbl1;
SELECT trainEntropyLearnedHash('id1') FROM tbl1 ORDER BY x;
SELECT entropyLearnedHash(x, 'id1') FROM tbl1 ORDER BY x;

-- with commonalities between keys
SELECT '---';
DROP TABLE IF EXISTS tbl2;
CREATE TABLE tbl2 (x String, y String) ENGINE=Memory;
INSERT INTO tbl2 VALUES ('ada', 'aa'), ('bda', 'ba'), ('adb', 'ab');
SELECT prepareTrainEntropyLearnedHash(x, 'id2') FROM tbl2;
SELECT trainEntropyLearnedHash('id2') FROM tbl2;
-- the following two statements must have the same results
SELECT entropyLearnedHash(x, 'id2') FROM tbl2 ORDER BY x;
SELECT cityHash64(y) FROM tbl2 ORDER BY x;

-- first position 3 will be chosen, and then 2. this test checks that the array of positions it sorted.
SELECT '---';
DROP TABLE IF EXISTS tbl3;
CREATE TABLE tbl3 (x String, y String) ENGINE=Memory;
INSERT INTO tbl3 VALUES ('taa', 'aa'), ('tab', 'ab'), ('tac', 'ac'), ('tbc', 'bc');
SELECT prepareTrainEntropyLearnedHash(x, 'id3') FROM tbl3;
SELECT trainEntropyLearnedHash('id3') FROM tbl3;
-- the following two statements must have the same results
SELECT entropyLearnedHash(x, 'id3') FROM tbl3 ORDER BY x;
SELECT cityHash64(y) FROM tbl3 ORDER BY x;


-- keys have different lengths
SELECT '---';
DROP TABLE IF EXISTS tbl4;
CREATE TABLE tbl4 (x String, y String) ENGINE=Memory;
INSERT INTO tbl4 VALUES ('a', 'a'), ('b', 'b'), ('acb', 'ab'), ('bca', 'ba');
SELECT prepareTrainEntropyLearnedHash(x, 'id4') FROM tbl4;
SELECT trainEntropyLearnedHash('id4') FROM tbl4;
-- the following two statements must have the same results
SELECT entropyLearnedHash(x, 'id4') FROM tbl4 ORDER BY x;
SELECT cityHash64(y) FROM tbl4 ORDER BY x;

-- negative tests
SELECT prepareTrainEntropyLearnedHash(x, 1) FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT prepareTrainEntropyLearnedHash(1, 'id1') FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT trainEntropyLearnedHash(1) FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT entropyLearnedHash(1, 'id1') FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT entropyLearnedHash(x, 'non-existing id') FROM tbl1; -- { serverError BAD_ARGUMENTS }

DROP TABLE tbl1;
DROP TABLE tbl2;
DROP TABLE tbl3;
DROP TABLE tbl4;
