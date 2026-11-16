-- Tags: replica, no-shared-merge-tree, no-replicated-database
-- - replica: uses ReplicatedMergeTree
-- - no-shared-merge-tree: relies on two explicit replicas + sequential quorum + STOP FETCHES to keep quorum unsatisfied
-- - no-replicated-database: creates two explicit replicas (r1, r2) sharing one ZooKeeper path

-- A quorum INSERT must honor the query time limit / cancellation while waiting for quorum, instead of blocking
-- for the whole insert_quorum_timeout. See ReplicatedMergeTreeSink::waitForQuorum: the quorum watch is only
-- signalled when the quorum node changes, so a killed query (or one that exceeded max_execution_time with
-- timeout_overflow_mode = 'throw') would never be woken up. Before the fix such an INSERT lingered in the
-- processlist for the entire (possibly very large) insert_quorum_timeout, tripping the hung-check.
--
-- The second replica never fetches the part (SYSTEM STOP FETCHES), so insert_quorum = 2 can never be satisfied.
-- Two timeout_overflow_mode values are covered, each on its own table (a finished-but-unsatisfied quorum write
-- would otherwise make the next INSERT fail early with UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE before it ever waits
-- for quorum):
--   - 'throw' (default): checkTimeLimit() throws directly, so insert_quorum_timeout is set very large while
--     max_execution_time is small and the INSERT must be interrupted promptly with TIMEOUT_EXCEEDED rather than
--     hanging for insert_quorum_timeout.
--   - 'break': checkTimeLimit() returns false instead of throwing. A quorum INSERT cannot report a partial
--     success, so waitForQuorum ignores the 'break'-mode timeout and the wait is bounded by insert_quorum_timeout
--     instead, ending with UNKNOWN_STATUS_OF_INSERT. (A 'KILL QUERY' is still honored every step regardless of the
--     mode -- see 04338_kill_quorum_insert.)

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
-- async_insert = 0 so this is a synchronous quorum INSERT regardless of the run configuration (the AsyncInsert
-- stateless runs force async_insert = 1, and async inserts with quorum require insert_quorum_parallel = 1).
INSERT INTO quorum_throw_r1
SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 6000000, max_execution_time = 3, timeout_overflow_mode = 'throw', async_insert = 0, insert_keeper_fault_injection_probability = 0
VALUES (1); -- { serverError TIMEOUT_EXCEEDED }

-- timeout_overflow_mode = 'break': checkTimeLimit() returns false instead of throwing, so the 'break'-mode
-- max_execution_time is ignored in the quorum wait and the INSERT is bounded by insert_quorum_timeout (set small
-- here), failing with UNKNOWN_STATUS_OF_INSERT. max_execution_time is set much larger than insert_quorum_timeout
-- so the quorum timeout fires first and the result is deterministic: a small 'break'-mode max_execution_time could
-- otherwise be consumed during pipeline setup and let the executor stop gracefully before reaching waitForQuorum.
INSERT INTO quorum_break_r1
SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 3000, max_execution_time = 300, timeout_overflow_mode = 'break', async_insert = 0, insert_keeper_fault_injection_probability = 0
VALUES (1); -- { serverError UNKNOWN_STATUS_OF_INSERT }

DROP TABLE quorum_throw_r1 SYNC;
DROP TABLE quorum_throw_r2 SYNC;
DROP TABLE quorum_break_r1 SYNC;
DROP TABLE quorum_break_r2 SYNC;
