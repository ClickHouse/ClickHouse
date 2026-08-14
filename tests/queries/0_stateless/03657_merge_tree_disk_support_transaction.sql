-- Tags: no-ordinary-database, no-fasttest, no-encrypted-storage, no-async-insert

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x;
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_disk';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_disk_2';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_disk_3';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='s3_disk';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='s3_plain_rewritable';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1); -- { serverError NOT_IMPLEMENTED }
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t  (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='s3_cache';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t  (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_plain_rewritable';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1); -- { serverError NOT_IMPLEMENTED }
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='s3_plain_rewritable_cache';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1); -- { serverError NOT_IMPLEMENTED }
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='s3_plain_rewritable_cache_multi';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1); -- { serverError NOT_IMPLEMENTED }
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_cache';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='local_cache_multi';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1);
SET implicit_transaction=False;

CREATE OR REPLACE TABLE t (x INT) ENGINE=MergeTree ORDER BY x SETTINGS disk='encrypted_s3_plain_rewritable_cache';
SET implicit_transaction=True;
INSERT INTO TABLE t VALUES (1); -- { serverError NOT_IMPLEMENTED }