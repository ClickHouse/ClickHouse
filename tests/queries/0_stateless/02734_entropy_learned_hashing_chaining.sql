DROP TABLE IF EXISTS tbl_train_1_arg;
CREATE TABLE tbl_train_1_arg (x String) ENGINE=Memory;
INSERT INTO tbl_train_1_arg VALUES ('ada'), ('bda'), ('adb');
SELECT trainEntropyLearnedHash(x) FROM tbl_train_1_arg;

DROP TABLE tbl_train_1_arg;
