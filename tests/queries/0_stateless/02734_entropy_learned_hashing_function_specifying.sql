DROP TABLE IF EXISTS tbl_cityhash;
CREATE TABLE tbl_cityhash (x String) ENGINE=Memory;
INSERT INTO tbl_cityhash VALUES ('a'), ('b'), ('c');
SELECT trainEntropyLearnedHash(x, 'id_cityhash') FROM tbl_cityhash;
SELECT entropyLearnedHash(x, 'id_cityhash', 'cityHash64') FROM tbl_cityhash;
SELECT farmHash64(x) FROM tbl_cityhash;

DROP TABLE IF EXISTS tbl_farmhash;
CREATE TABLE tbl_farmhash (x String) ENGINE=Memory;
INSERT INTO tbl_farmhash VALUES ('a'), ('b'), ('c');
SELECT trainEntropyLearnedHash(x, 'id_farmhash') FROM tbl_farmhash;
SELECT entropyLearnedHash(x, 'id_farmhash', 'farmHash64') FROM tbl_farmhash;
SELECT farmHash64(x) FROM tbl_farmhash;

DROP TABLE IF EXISTS tbl_siphash;
CREATE TABLE tbl_siphash (x String) ENGINE=Memory;
INSERT INTO tbl_siphash VALUES ('a'), ('b'), ('c');
SELECT trainEntropyLearnedHash(x, 'id_siphash') FROM tbl_siphash;
SELECT entropyLearnedHash(x, 'id_siphash', 'sipHash64') FROM tbl_siphash;
SELECT sipHash64(x) FROM tbl_siphash;

DROP TABLE tbl_cityhash;
DROP TABLE tbl_farmhash;
DROP TABLE tbl_siphash;
