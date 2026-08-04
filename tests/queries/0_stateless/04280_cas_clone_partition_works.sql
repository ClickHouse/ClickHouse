-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- Positive test for the part-cloning partition commands on a cas disk.
--
-- History: these commands (MOVE PARTITION ... TO TABLE, REPLACE PARTITION, ATTACH PARTITION ... FROM,
-- plain ATTACH PARTITION of a table's own detached parts) USED to be rejected with SUPPORT_IS_DISABLED
-- on CA, because the file-by-file `createHardLink` clone path had no enclosing transaction and would
-- corrupt the clone. CAS M9 W2 made that path transactional: `DataPartStorageOnDiskBase::freeze` runs
-- the whole clone through ONE CA transaction and `moveDirectory` re-keys the detached-staging → active
-- rename into a complete active ref. So the commands now SUCCEED and read back identical data. This test
-- locks that they work and produce the correct rows (the gate at `checkAlterPartitionIsPossible` for the
-- ContentAddressed metadata type now lists them as supported).
--
-- The tables use the DEFAULT MergeTree storage so this exercises whatever default disk the job installs:
-- on the local-CA job that is a local cas disk, on the cas-over-S3 job it is
-- a CA disk backed by minio. Both are the supported same-disk clone path.

DROP TABLE IF EXISTS t_cas_clone_src;
DROP TABLE IF EXISTS t_cas_clone_dst;

CREATE TABLE t_cas_clone_src (a UInt64, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY a;
CREATE TABLE t_cas_clone_dst (a UInt64, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY a;

INSERT INTO t_cas_clone_src SELECT number, 1 FROM numbers(100);
INSERT INTO t_cas_clone_src SELECT number, 2 FROM numbers(50);
INSERT INTO t_cas_clone_src SELECT number, 3 FROM numbers(30);

-- REPLACE PARTITION clones parts from another table into the destination.
ALTER TABLE t_cas_clone_dst REPLACE PARTITION 1 FROM t_cas_clone_src;
SELECT 'after_replace_dst_p1', count(), sum(a) FROM t_cas_clone_dst WHERE p = 1;

-- ATTACH PARTITION ... FROM clones parts from another table (parses to REPLACE_PARTITION, replace=false).
ALTER TABLE t_cas_clone_dst ATTACH PARTITION 2 FROM t_cas_clone_src;
SELECT 'after_attach_from_dst_p2', count(), sum(a) FROM t_cas_clone_dst WHERE p = 2;

-- MOVE PARTITION ... TO TABLE clones a partition to the destination and drops it from the source.
ALTER TABLE t_cas_clone_src MOVE PARTITION 3 TO TABLE t_cas_clone_dst;
SELECT 'after_move_dst_p3', count(), sum(a) FROM t_cas_clone_dst WHERE p = 3;
SELECT 'after_move_src_p3', count() FROM t_cas_clone_src WHERE p = 3;

-- Plain ATTACH PARTITION of the table's own detached part re-clones it back.
ALTER TABLE t_cas_clone_src DETACH PARTITION 1;
SELECT 'after_detach_src', count() FROM t_cas_clone_src;
ALTER TABLE t_cas_clone_src ATTACH PARTITION 1;
SELECT 'after_reattach_src', count(), sum(a) FROM t_cas_clone_src WHERE p = 1;

-- The pointer-unlink command DROP PARTITION still works.
ALTER TABLE t_cas_clone_src DROP PARTITION 2;
SELECT 'after_drop_src_p2', count() FROM t_cas_clone_src WHERE p = 2;

DROP TABLE t_cas_clone_src;
DROP TABLE t_cas_clone_dst;
SELECT 'dropped_ok';
