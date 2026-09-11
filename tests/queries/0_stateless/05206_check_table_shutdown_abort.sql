-- Tags: zookeeper, no-parallel, no-shared-merge-tree
-- A replica that has lost its Keeper session enters partial shutdown, and `checkDataNext` then throws
-- `ABORTED` without reading anything. That was turned into a failed check result with an empty part
-- path, so `CHECK TABLE ... SETTINGS check_query_single_value_result = 1` answered 0 - the value that
-- means "the data is broken" - on a healthy table, and stopped checking the remaining parts. A check
-- that could not run has to fail instead.
-- The `check_table_inject_shutdown_abort` failpoint is server-global, hence no-parallel, and it lives
-- in `StorageReplicatedMergeTree::checkDataNext`, hence no-shared-merge-tree.

SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_05206 SYNC;

CREATE TABLE t_05206 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05206', 'r1')
ORDER BY a;

INSERT INTO t_05206 VALUES (1);
INSERT INTO t_05206 VALUES (2);

-- The parts are healthy.
CHECK TABLE t_05206;

-- Abort the check the way a partial shutdown does.
SYSTEM ENABLE FAILPOINT check_table_inject_shutdown_abort;

CHECK TABLE t_05206; -- { serverError ABORTED }

-- The failpoint is ONCE, so it is already consumed, and the healthy parts check out again.
CHECK TABLE t_05206;
SELECT count() FROM t_05206;

DROP TABLE t_05206 SYNC;
