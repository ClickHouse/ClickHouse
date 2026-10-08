-- Tags: shard

-- A columnless remote()/Remote/Distributed table infers its schema from the remote side. If the
-- inference yields a Nullable(Nothing) column (e.g. a view doing SELECT NULL), the older behaviour
-- persisted it and then failed to load the table on the next restart, poisoning DB startup
-- (issue #111242). A fresh CREATE now rejects such a column, matching the check the load path runs.
-- ATTACH/RESTORE and temporary tables are unaffected: they either replay definitions already on
-- disk or are never persisted.

DROP VIEW IF EXISTS v_null_04626;
DROP TABLE IF EXISTS t_astf_04626;
DROP TABLE IF EXISTS t_remote_04626;
DROP TABLE IF EXISTS t_dist_04626;
DROP TABLE IF EXISTS base_04626;
DROP TABLE IF EXISTS t_ok_04626;

CREATE VIEW v_null_04626 AS SELECT NULL AS c0;

-- Fresh columnless CREATE AS remote() over the Nullable(Nothing) view: rejected at CREATE.
CREATE TABLE t_astf_04626 AS remote('127.0.0.1', currentDatabase(), 'v_null_04626'); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES }

-- Fresh columnless ENGINE = Remote over the same view (the reproducer from the issue): rejected at CREATE.
CREATE TABLE t_remote_04626 ENGINE = Remote('127.0.0.1', currentDatabase(), 'v_null_04626'); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES }

-- Fresh columnless ENGINE = Distributed over the same view: rejected at CREATE.
CREATE TABLE t_dist_04626 ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'v_null_04626'); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES }

-- None of the poisoned tables must have been persisted.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('t_astf_04626', 't_remote_04626', 't_dist_04626');

-- Control: columnless remote() over a normal table still works.
CREATE TABLE base_04626 (a UInt32, b String) ENGINE = Memory;
INSERT INTO base_04626 VALUES (1, 'x');
CREATE TABLE t_ok_04626 AS remote('127.0.0.1', currentDatabase(), 'base_04626');
SELECT a, b FROM t_ok_04626 ORDER BY a;

DROP TABLE t_ok_04626;
DROP TABLE base_04626;
DROP VIEW v_null_04626;
