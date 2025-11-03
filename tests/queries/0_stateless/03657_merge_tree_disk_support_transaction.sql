-- Tags: no-ordinary-database, no-fasttest, no-encrypted-storage, no-async-insert

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x;
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_disk';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (2);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_disk_2';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (3);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_disk_3';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (4);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='s3_disk';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (5);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='s3_plain_rewritable';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (6); -- { serverError NOT_IMPLEMENTED }
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t  (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='s3_cache';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (7);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t  (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_plain_rewritable';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (8); -- { serverError NOT_IMPLEMENTED }
SET implicit_transaction=False;
