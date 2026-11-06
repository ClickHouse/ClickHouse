-- CLEAR COLUMN of a column that a MATERIALIZED key column (sorting key, partition key, sign or version) is computed from
-- is rejected. A CLEAR that is already queued keeps the key column's stored value. Other MATERIALIZED columns are recalculated.

-- Sorting key.
DROP TABLE IF EXISTS t_order;
CREATE TABLE t_order (x Int32, y Int32, mx Int32 MATERIALIZED x + y) ENGINE = MergeTree ORDER BY mx;
INSERT INTO t_order (x, y) VALUES (100, 1), (1, 50), (2, 60), (200, 3);
ALTER TABLE t_order CLEAR COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'order', groupArray((x, y, mx)) FROM (SELECT x, y, mx FROM t_order ORDER BY mx);

-- Partition key, also for one partition.
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (x Int32, y Int32, p Int32 MATERIALIZED x) ENGINE = MergeTree PARTITION BY p ORDER BY y;
INSERT INTO t_partition (x, y) VALUES (5, 1), (5, 2);
ALTER TABLE t_partition CLEAR COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_partition CLEAR COLUMN x IN PARTITION 5; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'partition', groupArray((x, y, p)) FROM (SELECT x, y, p FROM t_partition ORDER BY y);

-- Sorting key on a subcolumn of a MATERIALIZED column.
DROP TABLE IF EXISTS t_subcolumn;
CREATE TABLE t_subcolumn (x Int32, y Int32, m Tuple(a Int32) MATERIALIZED tuple(x + y)) ENGINE = MergeTree ORDER BY m.a;
INSERT INTO t_subcolumn (x, y) VALUES (100, 1), (200, 3);
ALTER TABLE t_subcolumn CLEAR COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'subcolumn', groupArray((x, y, m.a)) FROM (SELECT x, y, m FROM t_subcolumn ORDER BY m.a);

-- Key reached through a non-key MATERIALIZED column, and CLEAR of that column itself.
DROP TABLE IF EXISTS t_chain;
CREATE TABLE t_chain (x Int32, y Int32, i Int32 MATERIALIZED x * 2, k Int32 MATERIALIZED i + y) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_chain (x, y) VALUES (100, 1), (200, 3);
ALTER TABLE t_chain CLEAR COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_chain CLEAR COLUMN i; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'chain', groupArray((x, y, i, k)) FROM (SELECT x, y, i, k FROM t_chain ORDER BY k);

-- Version column of ReplacingMergeTree.
DROP TABLE IF EXISTS t_version;
CREATE TABLE t_version (id Int32, x Int32, v UInt32 MATERIALIZED toUInt32(x)) ENGINE = ReplacingMergeTree(v) ORDER BY id;
INSERT INTO t_version (id, x) VALUES (1, 10);
ALTER TABLE t_version CLEAR COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'version', groupArray((id, x, v)) FROM t_version;

-- Sorting key column that older parts do not store.
DROP TABLE IF EXISTS t_absent;
CREATE TABLE t_absent (a UInt32, x UInt32, y UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_absent VALUES (1, 10, 10), (1, 30, 1);
ALTER TABLE t_absent ADD COLUMN k UInt32, MODIFY ORDER BY (a, k);
ALTER TABLE t_absent MODIFY COLUMN k UInt32 MATERIALIZED x + y;
INSERT INTO t_absent (a, x, y) VALUES (2, 5, 5);
ALTER TABLE t_absent CLEAR COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'absent', groupArray((a, x, y, k)) FROM (SELECT a, x, y, k FROM t_absent ORDER BY a, k);
OPTIMIZE TABLE t_absent FINAL;
SELECT 'absent after optimize', groupArray((a, x, y, k)) FROM (SELECT a, x, y, k FROM t_absent ORDER BY a, k);

-- A CLEAR queued before the key column depended on the cleared column keeps the key column's stored value.
DROP TABLE IF EXISTS t_late;
CREATE TABLE t_late (x Int32, y Int32, z Int32, mx Int32 MATERIALIZED -y, w Int32 MATERIALIZED x + 1) ENGINE = MergeTree ORDER BY mx;
INSERT INTO t_late (x, y, z) VALUES (100, 1, 0), (200, 3, 0);
SYSTEM STOP MERGES t_late;
ALTER TABLE t_late CLEAR COLUMN x SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_late MODIFY COLUMN mx Int32 MATERIALIZED x + y;
SYSTEM START MERGES t_late;
ALTER TABLE t_late UPDATE z = 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'late', groupArray((x, y, z, mx, w)) FROM (SELECT x, y, z, mx, w FROM t_late ORDER BY mx);
OPTIMIZE TABLE t_late FINAL;
SELECT 'late after optimize', groupArray((x, y, z, mx, w)) FROM (SELECT x, y, z, mx, w FROM t_late ORDER BY mx);

-- Not a key column: recalculated.
DROP TABLE IF EXISTS t_nonkey;
CREATE TABLE t_nonkey (x Int32, y Int32, z Int32 MATERIALIZED x + 1, mk Int32 MATERIALIZED y * 2) ENGINE = MergeTree ORDER BY mk;
INSERT INTO t_nonkey (x, y) VALUES (100, 1), (200, 3);
ALTER TABLE t_nonkey CLEAR COLUMN x SETTINGS mutations_sync = 2;
SELECT 'nonkey', groupArray((x, y, z, mk)) FROM (SELECT x, y, z, mk FROM t_nonkey ORDER BY mk);

SELECT 'unfinished mutations', count() FROM system.mutations WHERE database = currentDatabase() AND NOT is_done;
SELECT 'finished mutations', count() FROM system.mutations WHERE database = currentDatabase() AND is_done;

DROP TABLE t_order;
DROP TABLE t_partition;
DROP TABLE t_subcolumn;
DROP TABLE t_chain;
DROP TABLE t_version;
DROP TABLE t_absent;
DROP TABLE t_late;
DROP TABLE t_nonkey;
