-- The pipeline executors charge `additional_memory_tracking_per_thread` (4 MiB by
-- default) against the query's `MemoryTracker` for the lifetime of every job.
-- This test verifies that:
--   * an in-pipeline `MEMORY_LIMIT_EXCEEDED` lands inside the per-job `catch`
--     block and propagates as a normal query failure (no deadlock on the
--     output queue);
--   * the server setting is exposed in `system.server_settings`.
--
-- Stateless tests run with the production default (4 MiB) for the setting.
-- To make the failure path independent of that reservation, we force the
-- same exception via `max_untracked_memory = 0`: every allocation is
-- reported immediately, so the 1-byte query limit fails the first
-- in-query allocation inside the pipeline.

-- The server setting must be visible.
SELECT count() FROM system.server_settings WHERE name = 'additional_memory_tracking_per_thread';

-- A query whose `max_memory_usage` is tiny must fail with MEMORY_LIMIT_EXCEEDED
-- rather than hanging. The regression we are guarding against is the
-- pipeline's consumer blocking forever in `ConcurrentBoundedQueue::popImpl`
-- when the per-job lambda throws.
SELECT count() FROM numbers_mt(1000) SETTINGS max_threads = 4, max_memory_usage = 1, max_untracked_memory = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }
