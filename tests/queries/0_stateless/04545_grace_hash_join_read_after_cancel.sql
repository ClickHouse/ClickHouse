-- Tags: no-asan, no-tsan, no-msan
-- Tags: sanitizer memory overhead shifts the OOM point of this memory-limit-driven spill
-- failure and makes the exact limit flaky (same reason 03277_join_adaptive_spill_oom excludes
-- sanitizers). The chassert being guarded still fires in debug builds, which are not excluded.
--
-- A grace_hash join whose data spills to disk, then fails mid-read under a tight memory limit
-- while several DelayedJoinedBlocksWorkerTransform threads share one spilled-block reader.
-- Before the fix, a failing read left the shared reader canceled but not finished, so a sibling
-- worker re-read the canceled buffer and tripped chassert(!isCanceled()) ("ReadBuffer is
-- canceled. Can't read from it.", a server abort in debug/sanitizer builds). The query must fail
-- gracefully with MEMORY_LIMIT_EXCEEDED, never abort.
DROP TABLE IF EXISTS grace_hash_cancel_04545;

CREATE TABLE grace_hash_cancel_04545 (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO grace_hash_cancel_04545 SELECT number, repeat('x', 200) FROM numbers(400000);

SET join_algorithm = 'grace_hash';
SET grace_hash_join_initial_buckets = 4;
SET max_bytes_in_join = 600000;   -- force spilling to disk
SET max_threads = 16;             -- several workers share one spilled-block reader
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_memory_usage = 80000000;  -- fail a spilled read mid-join

-- Read l.s directly (not through count()): count() lets the analyzer prune the wide l.s column,
-- so the payload would never be materialized from the spilled left blocks and the shared-reader
-- path this PR fixes could go uncovered. Selecting l.s makes it a required output column.
SELECT l.s, l.k FROM grace_hash_cancel_04545 AS l JOIN grace_hash_cancel_04545 AS r ON l.k = r.k FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

DROP TABLE grace_hash_cancel_04545;
