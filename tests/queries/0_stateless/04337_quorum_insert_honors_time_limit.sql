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
-- The second replica never fetches the part (SYSTEM STOP FETCHES), so insert_quorum = 2 can never be satisfied.
-- insert_quorum_timeout is set very large while max_execution_time is small, so the INSERT must be interrupted
-- promptly with TIMEOUT_EXCEEDED rather than hanging for insert_quorum_timeout. Both timeout_overflow_mode values
-- are covered: checkTimeLimit() throws for 'throw' and only returns false for 'break', but a quorum INSERT cannot
-- report a partial success while the quorum status is unknown, so 'break' must error out too (it is escalated to a
-- hard timeout). Each mode uses its own table because a finished-but-unsatisfied quorum write would otherwise make
-- the next INSERT fail early with UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE before it ever waits for quorum.

DROP TABLE IF EXISTS quorum_throw_r1 SYNC;
DROP TABLE IF EXISTS quorum_throw_r2 SYNC;
DROP TABLE IF EXISTS quorum_break_r1 SYNC;
DROP TABLE IF EXISTS quorum_break_r2 SYNC;

CREATE TABLE quorum_throw_r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04337/throw', 'r1') ORDER BY x;
CREATE TABLE quorum_throw_r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04337/throw', 'r2') ORDER BY x;
CREATE TABLE quorum_break_r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04337/break', 'r1') ORDER BY x;
CREATE TABLE quorum_break_r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04337/break', 'r2') ORDER BY x;

SYSTEM STOP FETCHES quorum_throw_r2;
SYSTEM STOP FETCHES quorum_break_r2;

-- timeout_overflow_mode = 'throw' (default): checkTimeLimit() throws TIMEOUT_EXCEEDED directly.
INSERT INTO quorum_throw_r1
SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 6000000, max_execution_time = 3, timeout_overflow_mode = 'throw', insert_keeper_fault_injection_probability = 0
VALUES (1); -- { serverError TIMEOUT_EXCEEDED }

-- timeout_overflow_mode = 'break': checkTimeLimit() returns false; the wait must still stop and fail rather than
-- hang until insert_quorum_timeout or report a successful "break" with unknown quorum status.
INSERT INTO quorum_break_r1
SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 6000000, max_execution_time = 3, timeout_overflow_mode = 'break', insert_keeper_fault_injection_probability = 0
VALUES (1); -- { serverError TIMEOUT_EXCEEDED }

DROP TABLE quorum_throw_r1 SYNC;
DROP TABLE quorum_throw_r2 SYNC;
DROP TABLE quorum_break_r1 SYNC;
DROP TABLE quorum_break_r2 SYNC;
