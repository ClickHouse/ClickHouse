-- Tags: no-ordinary-database
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/74262
-- ALTER FREEZE marks a part's files (txn_version.txt included) read-only via make_source_readonly.
-- Later updating the removal TID of that frozen part must not open txn_version.txt in place,
-- otherwise the write fails with CANNOT_OPEN_FILE (errno 13, Permission denied) and aborts the server.

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();

-- Create the part inside a transaction so txn_version.txt is written to disk.
BEGIN TRANSACTION;
INSERT INTO t0 VALUES (1);
COMMIT;

-- Freeze strips the owner-write bit from every source file, including txn_version.txt.
ALTER TABLE t0 FREEZE WITH NAME 'f_03444';

-- Removing the frozen part inside a committed transaction rewrites its removal TID.
-- This used to open the read-only txn_version.txt directly and crash the server.
BEGIN TRANSACTION;
ALTER TABLE t0 DROP PARTITION ALL;
COMMIT;

SELECT count() FROM t0;

ALTER TABLE t0 UNFREEZE WITH NAME 'f_03444';
DROP TABLE t0;
