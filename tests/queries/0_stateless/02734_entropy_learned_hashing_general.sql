DROP TABLE IF EXISTS tbl_no_commonalities;
CREATE TABLE tbl_no_commonalities (x String) ENGINE=Memory;
-- no commonalities between keys
INSERT INTO tbl_no_commonalities VALUES ('a'), ('b'), ('c');
SELECT trainEntropyLearnedHash(x, 'id_no_commonalities') FROM tbl_no_commonalities;
SELECT entropyLearnedHash(x, 'id_no_commonalities') FROM tbl_no_commonalities;

SELECT trainEntropyLearnedHash(x, 1) FROM tbl_no_commonalities; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- SELECT trainEntropyLearnedHash(x, NULL) FROM tbl_no_commonalities; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT trainEntropyLearnedHash(1, 'id_no_commonalities') FROM tbl_no_commonalities; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT entropyLearnedHash(x, 'non-existing id') FROM tbl_no_commonalities; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS tbl_with_commonalities;
-- x -- train data, y -- right chosen symbols
CREATE TABLE tbl_with_commonalities (x String, y String) ENGINE=Memory;
-- with commonalities between keys
INSERT INTO tbl_with_commonalities VALUES ('ada', 'aa'), ('bda', 'ba'), ('adb', 'ab');
SELECT trainEntropyLearnedHash(x, 'id_with_commonalities') FROM tbl_with_commonalities;
-- these two calls must have equal results
SELECT entropyLearnedHash(x, 'id_with_commonalities') FROM tbl_with_commonalities;
SELECT cityHash64(y) FROM tbl_with_commonalities;

DROP TABLE IF EXISTS tbl_test_sorted;
CREATE TABLE tbl_test_sorted (x String, y String) ENGINE=Memory;
-- first position 3 will be chosen, and then 2. this test checks that the array of positions it sorted.
INSERT INTO tbl_test_sorted VALUES ('taa', 'aa'), ('tab', 'ab'), ('tac', 'ac'), ('tbc', 'bc');
SELECT trainEntropyLearnedHash(x, 'id_test_sorted') FROM tbl_test_sorted;
SELECT entropyLearnedHash(x, 'id_test_sorted') FROM tbl_test_sorted;
SELECT cityHash64(y) FROM tbl_test_sorted;

DROP TABLE IF EXISTS tbl_different_len;
CREATE TABLE tbl_different_len (x String, y String) ENGINE=Memory;
-- different length strings
INSERT INTO tbl_different_len VALUES ('a', 'a'), ('b', 'b'), ('acb', 'ab'), ('bca', 'ba');
SELECT trainEntropyLearnedHash(x, 'id_different_len') FROM tbl_different_len;
SELECT entropyLearnedHash(x, 'id_different_len') FROM tbl_different_len;
SELECT cityHash64(y) FROM tbl_different_len;


DROP TABLE tbl_no_commonalities;
DROP TABLE tbl_with_commonalities;
DROP TABLE tbl_test_sorted;
DROP TABLE tbl_different_len;
