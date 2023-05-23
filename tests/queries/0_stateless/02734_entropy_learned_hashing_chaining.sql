DROP TABLE IF EXISTS tbl_train_1_arg;
CREATE TABLE tbl_train_1_arg (x String) ENGINE=Memory;
INSERT INTO tbl_train_1_arg VALUES ('ada'), ('bda'), ('adb');
SELECT trainEntropyLearnedHash(x) FROM tbl_train_1_arg;

DROP TABLE IF EXISTS tbl_simple_bitmask;
CREATE TABLE tbl_simple_bitmask (x String) ENGINE=Memory;
INSERT INTO tbl_simple_bitmask VALUES ('ada'), ('bda'), ('adb');
SELECT entropyLearnedHash(x, '!101') FROM tbl_simple_bitmask;

DROP TABLE IF EXISTS tbl_chaining;
CREATE TABLE tbl_chaining (x String) ENGINE=Memory;
INSERT INTO tbl_chaining VALUES ('ada'), ('bda'), ('adb');
SELECT entropyLearnedHash(x, trainEntropyLearnedHash(x)) FROM tbl_chaining;

DROP TABLE tbl_train_1_arg;
DROP TABLE tbl_simple_bitmask;
DROP TABLE tbl_chaining;
