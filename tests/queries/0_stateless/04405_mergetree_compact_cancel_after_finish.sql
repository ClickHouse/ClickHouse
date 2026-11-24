-- Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
-- no-parallel: enables a server-global failpoint (replicated_merge_tree_insert_quorum_fail_0)
-- no-shared-merge-tree: the quorum INSERT path and the failpoint are specific to ReplicatedMergeTree
-- no-replicated-database: creates two explicit replicas (r1, r2) sharing one ZooKeeper path

-- Regression test for a crash introduced while fixing https://github.com/ClickHouse/ClickHouse/issues/56288.
--
-- MergeTreeDataPartWriterCompact::finishDataSerialization releases the data ('data.bin') and marks
-- ('data.cmrk*') file descriptors as soon as the part is flushed and synced, before the part is
-- committed. The writer's cancel() can still run afterwards: a quorum INSERT finalizes the part
-- (commits it locally), and if the following quorum step fails, the sink destructor calls cancel()
-- on the already-finished temporary part. cancel() must therefore tolerate the released (null)
-- streams instead of dereferencing them, otherwise the original error turns into a segfault in a
-- noexcept function during cleanup. The failpoint forces the quorum step to fail right after the
-- part has been finalized.

DROP TABLE IF EXISTS t_compact_cancel_r1 SYNC;
DROP TABLE IF EXISTS t_compact_cancel_r2 SYNC;

CREATE TABLE t_compact_cancel_r1 (a UInt64, b String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04405_cancel_after_finish', 'r1')
ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;  -- force a compact part

CREATE TABLE t_compact_cancel_r2 (a UInt64, b String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04405_cancel_after_finish', 'r2')
ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

-- The second replica never fetches the part, so quorum = 2 is reached but never satisfied.
SYSTEM STOP FETCHES t_compact_cancel_r2;

SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_quorum_fail_0;

-- The compact part is finalized (the writer releases its data/marks streams), then the forced fault
-- makes the quorum step throw, and the sink destructor cancels the finished part. Before the fix this
-- segfaulted in MergeTreeDataPartWriterCompact::cancel.
INSERT INTO t_compact_cancel_r1 SELECT number, toString(number) FROM numbers(100)
SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_keeper_max_retries = 0, insert_keeper_fault_injection_probability = 0; -- { serverError UNKNOWN_STATUS_OF_INSERT }

SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_quorum_fail_0;

-- The server must still be alive, and the locally committed part must be readable.
SELECT count() FROM t_compact_cancel_r1;

DROP TABLE t_compact_cancel_r1 SYNC;
DROP TABLE t_compact_cancel_r2 SYNC;
