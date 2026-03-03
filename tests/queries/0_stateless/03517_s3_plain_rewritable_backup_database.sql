-- Tags: no-fasttest, no-parallel, no-flaky-check, no-encrypted-storage
-- Because we are creating a backup with fixed path.

DROP DATABASE IF EXISTS d0 SYNC;
CREATE DATABASE d0 ENGINE = Atomic;

CREATE TABLE d0.t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();

BACKUP DATABASE d0 TO Disk('disk_s3_plain_rewritable_03517', 'backup6') FORMAT Null;
