-- Tags: no-fasttest, no-shared-merge-tree
-- Tag no-fasttest: requires S3
-- Tag no-shared-merge-tree: does not support replication

DROP TABLE IF EXISTS t0 SYNC;

CREATE TABLE t0 (c0 Int32) ENGINE = MergeTree() ORDER BY c0
SETTINGS disk='disk_encrypted_03517';

INSERT INTO t0 VALUES (1), (2), (3);

SELECT * FROM t0;

DROP TABLE t0;
