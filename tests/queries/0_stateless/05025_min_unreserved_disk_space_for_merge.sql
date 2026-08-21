-- Tags: no-object-storage, no-replicated-database
-- no-object-storage: the setting has no effect on disks with unlimited space.
-- no-replicated-database: OPTIMIZE on replicated tables bypasses the selection-time cap.
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
