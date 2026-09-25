-- Tags: zookeeper, no-parallel, no-shared-merge-tree
-- A replica that has lost its Keeper session enters partial shutdown, and `checkDataNext` then throws
-- `ABORTED` without reading anything. That was turned into a failed check result with an empty part
-- path, so `CHECK TABLE ... SETTINGS check_query_single_value_result = 1` answered 0 - the value that
-- means "the data is broken" - on a healthy table, and stopped checking the remaining parts. A check
-- that could not run has to fail instead.
-- The same partial shutdown also stops `part_check_thread`, which cancels a part check that is already
-- running; that used to come back as a failed check of a healthy part and has to fail the query too.
-- The failpoints are server-global, hence no-parallel, and they live in `StorageReplicatedMergeTree`
-- and `ReplicatedMergeTreePartCheckThread`, hence no-shared-merge-tree.

SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_05227 SYNC;

CREATE TABLE t_05227 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05227', 'r1')
ORDER BY a;

INSERT INTO t_05227 VALUES (1);
INSERT INTO t_05227 VALUES (2);

-- The parts are healthy.
CHECK TABLE t_05227;

-- Abort the check before it starts, the way a partial shutdown does.
SYSTEM ENABLE FAILPOINT check_table_inject_shutdown_abort;

CHECK TABLE t_05227; -- { serverError ABORTED }

-- The failpoint is ONCE, so it is already consumed, and the healthy parts check out again.
CHECK TABLE t_05227;

-- Cancel the check of a part that is already being checked, the way `part_check_thread.stop()` does
-- during a partial shutdown.
SYSTEM ENABLE FAILPOINT check_table_inject_part_check_cancelled;

CHECK TABLE t_05227; -- { serverError ABORTED }

CHECK TABLE t_05227;
SELECT count() FROM t_05227;

DROP TABLE t_05227 SYNC;
