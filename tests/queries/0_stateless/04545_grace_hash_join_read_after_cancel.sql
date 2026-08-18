-- Tags: no-parallel, no-fasttest
-- no-parallel: the failpoint is server-global.
-- no-fasttest: the Fast test build has no libfiu, so SYSTEM ENABLE FAILPOINT is unsupported.
--
-- A grace_hash join spills to disk, then one shared spilled-block reader fails mid-read while
-- several DelayedJoinedBlocksWorkerTransform workers hold it. Before the fix the failing read
-- left the underlying ReadBuffer canceled but the reader not finished, so a sibling worker
-- re-entered read() on the canceled buffer and tripped chassert(!isCanceled()) ("ReadBuffer is
-- canceled. Can't read from it.", a server abort in debug/sanitizer builds). The query must fail
-- with the injected error only, and the server must stay alive.

DROP TABLE IF EXISTS grace_hash_failpoint_04545;

CREATE TABLE grace_hash_failpoint_04545 (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO grace_hash_failpoint_04545 SELECT number, repeat('x', 200) FROM numbers(400000);

SET join_algorithm = 'grace_hash';
SET grace_hash_join_initial_buckets = 4;
SET max_bytes_in_join = 600000;   -- force spilling to disk
SET max_threads = 16;             -- several workers share one spilled-block reader
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_joins = 0;

-- DelayedJoinedBlocksTransform::prepare hands the same delayed stream to every worker port before
-- any worker runs, so by the time this fires the reader is already shared. The failpoint is armed
-- only for the left (delayed) reader, so it cannot fire on the single-threaded build phase.
SYSTEM ENABLE FAILPOINT grace_hash_join_fail_in_delayed_block_read;

SELECT l.s, l.k FROM grace_hash_failpoint_04545 AS l JOIN grace_hash_failpoint_04545 AS r ON l.k = r.k FORMAT Null; -- { serverError FAULT_INJECTED }

SYSTEM DISABLE FAILPOINT grace_hash_join_fail_in_delayed_block_read;

SELECT 'server alive';

DROP TABLE grace_hash_failpoint_04545;
