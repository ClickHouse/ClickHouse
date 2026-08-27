-- Tags: no-object-storage, zookeeper, no-shared-merge-tree
-- no-object-storage: the setting has no effect on disks with unlimited space.
-- no-shared-merge-tree: the replicated section covers StorageReplicatedMergeTree's own OPTIMIZE path.
-- https://github.com/ClickHouse/ClickHouse/issues/80006

SET optimize_throw_if_noop = 1;

DROP TABLE IF EXISTS t_min_unreserved;
-- 1 PiB of protected space exceeds any real CI disk, so merges must not be selected.
CREATE TABLE t_min_unreserved (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS min_unreserved_disk_space_for_merge = 1125899906842624;

INSERT INTO t_min_unreserved VALUES (1);
INSERT INTO t_min_unreserved VALUES (2);

OPTIMIZE TABLE t_min_unreserved; -- { serverError CANNOT_ASSIGN_OPTIMIZE }
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_min_unreserved' AND active;

ALTER TABLE t_min_unreserved MODIFY SETTING min_unreserved_disk_space_for_merge = 0;

OPTIMIZE TABLE t_min_unreserved;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_min_unreserved' AND active;

DROP TABLE t_min_unreserved;

-- The replicated OPTIMIZE path derives the same cap, so it refuses at selection time
-- instead of enqueueing an entry the queue would then postpone forever.
DROP TABLE IF EXISTS t_min_unreserved_rep;
CREATE TABLE t_min_unreserved_rep (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_min_unreserved_rep', '1') ORDER BY x
    SETTINGS min_unreserved_disk_space_for_merge = 1125899906842624;

INSERT INTO t_min_unreserved_rep VALUES (1);
INSERT INTO t_min_unreserved_rep VALUES (2);

OPTIMIZE TABLE t_min_unreserved_rep; -- { serverError CANNOT_ASSIGN_OPTIMIZE }
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_min_unreserved_rep' AND active;

ALTER TABLE t_min_unreserved_rep MODIFY SETTING min_unreserved_disk_space_for_merge = 0;

OPTIMIZE TABLE t_min_unreserved_rep;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_min_unreserved_rep' AND active;

DROP TABLE t_min_unreserved_rep;

-- OPTIMIZE with FINAL or with an explicit PARTITION bypasses the cap, so the protected
-- headroom never blocks a merge the user asked for.
DROP TABLE IF EXISTS t_min_unreserved_final;
CREATE TABLE t_min_unreserved_final (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS min_unreserved_disk_space_for_merge = 1125899906842624;

INSERT INTO t_min_unreserved_final VALUES (1);
INSERT INTO t_min_unreserved_final VALUES (2);

OPTIMIZE TABLE t_min_unreserved_final FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_min_unreserved_final' AND active;

DROP TABLE t_min_unreserved_final;

-- Same for the replicated engine: the queue must not re-apply the cap to these entries,
-- otherwise it postpones them forever and the query hangs.
DROP TABLE IF EXISTS t_min_unreserved_rep_final;
CREATE TABLE t_min_unreserved_rep_final (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_min_unreserved_rep_final', '1') ORDER BY x
    SETTINGS min_unreserved_disk_space_for_merge = 1125899906842624;

INSERT INTO t_min_unreserved_rep_final VALUES (1);
INSERT INTO t_min_unreserved_rep_final VALUES (2);

OPTIMIZE TABLE t_min_unreserved_rep_final PARTITION tuple();
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_min_unreserved_rep_final' AND active;

INSERT INTO t_min_unreserved_rep_final VALUES (3);

OPTIMIZE TABLE t_min_unreserved_rep_final FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_min_unreserved_rep_final' AND active;

DROP TABLE t_min_unreserved_rep_final;
