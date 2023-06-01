-- Tags: no-parallel
-- no-parallel because entropy-learned hash uses global state

SET allow_experimental_hash_functions = 1;

-- no commonalities between keys
DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (x String) ENGINE=Memory;
INSERT INTO tbl1 VALUES ('a'), ('b'), ('c');
SELECT prepareTrainEntropyLearnedHash(x, 'id1') FROM tbl1;
SELECT trainEntropyLearnedHash('id1') FROM tbl1;
SELECT entropyLearnedHash(x, 'id1') FROM tbl1;

-- with commonalities between keys
DROP TABLE IF EXISTS tbl2;
CREATE TABLE tbl2 (x String) ENGINE=Memory;
INSERT INTO tbl2 VALUES ('aa'), ('ba'), ('ca');
SELECT prepareTrainEntropyLearnedHash(x, 'id2') FROM tbl2;
SELECT trainEntropyLearnedHash('id2') FROM tbl2;
SELECT entropyLearnedHash(x, 'id2') FROM tbl2;

-- negative tests
SELECT prepareTrainEntropyLearnedHash(x, 1) FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT prepareTrainEntropyLearnedHash(1, 'id1') FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT trainEntropyLearnedHash(1) FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT entropyLearnedHash(1, 'id1') FROM tbl1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT entropyLearnedHash(x, 'non-existing id') FROM tbl1; -- { serverError BAD_ARGUMENTS }

DROP TABLE tbl1;
DROP TABLE tbl2;
