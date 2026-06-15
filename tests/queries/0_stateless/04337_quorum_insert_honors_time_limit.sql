-- Tags: replica, no-shared-merge-tree, no-replicated-database
-- - replica: uses ReplicatedMergeTree
-- - no-shared-merge-tree: relies on two explicit replicas + sequential quorum + STOP FETCHES to keep quorum unsatisfied
-- - no-replicated-database: creates two explicit replicas (r1, r2) sharing one ZooKeeper path

-- A quorum INSERT must honor the query time limit / cancellation while waiting for quorum, instead of blocking
-- for the whole insert_quorum_timeout. See ReplicatedMergeTreeSink::waitForQuorum: the quorum watch is only
-- signalled when the quorum node changes, so a killed query (or one that exceeded max_execution_time) would never
-- be woken up. Before the fix such an INSERT lingered in the processlist for the entire (possibly very large)
-- insert_quorum_timeout, tripping the hung-check.
--
-- Here the second replica never fetches the part (SYSTEM STOP FETCHES), so insert_quorum = 2 can never be
-- satisfied. insert_quorum_timeout is set very large, while max_execution_time is small: the INSERT must be
-- interrupted promptly with TIMEOUT_EXCEEDED rather than hanging for insert_quorum_timeout.

DROP TABLE IF EXISTS quorum_r1 SYNC;
DROP TABLE IF EXISTS quorum_r2 SYNC;

CREATE TABLE quorum_r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04337/quorum_cancel', 'r1') ORDER BY x;
CREATE TABLE quorum_r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04337/quorum_cancel', 'r2') ORDER BY x;

-- r2 stays active (so the quorum precondition passes) but never fetches the part, so quorum = 2 is never reached.
SYSTEM STOP FETCHES quorum_r2;

INSERT INTO quorum_r1
SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 6000000, max_execution_time = 3, insert_keeper_fault_injection_probability = 0
VALUES (1); -- { serverError TIMEOUT_EXCEEDED }

DROP TABLE quorum_r1 SYNC;
DROP TABLE quorum_r2 SYNC;
