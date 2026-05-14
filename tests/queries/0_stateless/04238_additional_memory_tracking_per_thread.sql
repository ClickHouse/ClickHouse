-- The pipeline executors charge `additional_memory_tracking_per_thread` (4 MiB by
-- default) against the query's `MemoryTracker` for the lifetime of every job.
-- This test verifies the charge:
--   * is applied after the thread group is attached, so the throw lands inside
--     the pipeline's per-job `catch` block and propagates as a normal query
--     failure (no deadlock on the output queue);
--   * actually fires when the query-level limit cannot accommodate it.

-- The server setting must be visible.
SELECT count() FROM system.server_settings WHERE name = 'additional_memory_tracking_per_thread';

-- A query whose `max_memory_usage` is much smaller than the speculative
-- reservation must fail with MEMORY_LIMIT_EXCEEDED rather than hanging.
-- This holds whether the speculative reservation itself trips the limit, or a
-- subsequent in-query allocation does -- the regression we are guarding against
-- is the pipeline's consumer blocking forever in `ConcurrentBoundedQueue::popImpl`.
SELECT count() FROM numbers_mt(1000) SETTINGS max_threads = 4, max_memory_usage = 1; -- { serverError MEMORY_LIMIT_EXCEEDED }
