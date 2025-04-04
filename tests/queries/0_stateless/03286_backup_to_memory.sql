DROP TABLE IF EXISTS t1;

CREATE TABLE t1(x Int32) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO t1 VALUES (1), (2), (3);

BACKUP TABLE t1 TO Memory('b1') FORMAT Null;
DROP TABLE t1 SYNC;
RESTORE TABLE t1 FROM Memory('b1') FORMAT Null;

SELECT * FROM t1 ORDER BY x;

DROP TABLE t1;
