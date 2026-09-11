-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104309
--
-- `FailedInternalQuery` / `FailedInternalInsertQuery` / `FailedInternalSelectQuery` were
-- only incremented in `logQueryException` (failures during execution) but not in
-- `logExceptionBeforeStart` (failures before execution starts), undercounting internal
-- failures: async insert flushes, materialized view refreshes, and parse errors with
-- `internal=true`.
--
-- This test triggers an async insert flush failure (which calls
-- `logExceptionBeforeStart` from `AsynchronousInsertQueue::processData` with
-- `internal=true`) and verifies that the relevant internal counters for the INSERT
-- path (`FailedInternalQuery` and `FailedInternalInsertQuery`) are bumped alongside
-- the existing user-visible `FailedQuery` and `FailedInsertQuery` increments.
-- `FailedInternalSelectQuery` is not asserted here because the failing path is INSERT.

DROP TABLE IF EXISTS t_failed_internal_async_flush;
CREATE TABLE t_failed_internal_async_flush (a UInt32) ENGINE = MergeTree ORDER BY a;

-- Snapshot the relevant counters. `system.events` only exposes events that have been
-- incremented at least once, so absent rows are treated as zero in the delta below.
DROP TEMPORARY TABLE IF EXISTS t_snap_before;
CREATE TEMPORARY TABLE t_snap_before AS
SELECT event, value FROM system.events
WHERE event IN (
    'FailedQuery',
    'FailedInsertQuery',
    'FailedInternalQuery',
    'FailedInternalInsertQuery'
);

-- Use a very long busy timeout so the async insert queue does NOT auto-flush our
-- entries before we drop the table. The explicit table-scoped `SYSTEM FLUSH ASYNC
-- INSERT QUEUE` below is what triggers the failure deterministically. Disable the
-- adaptive timeout so the queue cannot decide to drain early.
SET async_insert = 1;
SET wait_for_async_insert = 0;
SET async_insert_use_adaptive_busy_timeout = 0;
SET async_insert_busy_timeout_min_ms = 600000;
SET async_insert_busy_timeout_max_ms = 600000;

INSERT INTO t_failed_internal_async_flush VALUES (1);
INSERT INTO t_failed_internal_async_flush VALUES (2);
INSERT INTO t_failed_internal_async_flush VALUES (3);

DROP TABLE t_failed_internal_async_flush;

-- Force the queue to drain so the failure is observed before we read the counters.
-- IMPORTANT: this must be the table-scoped variant (`SYSTEM FLUSH ASYNC INSERT QUEUE
-- <table>`). The unscoped variant calls `flushAll()` which sets `flush_stopped=true`
-- and then `pool.wait()`s for *every* in-flight entry across the whole queue. In
-- parallel CI configurations (Fast test, etc.) the queue regularly contains
-- entries belonging to other tests whose inserts go through MV/Distributed
-- pipelines that block on remote shards for up to
-- `wait_for_async_insert_timeout` (120s). Picking those up turns this test into
-- a several-minute serializer that times out 10+ unrelated async-insert tests.
-- The table-scoped variant calls `flush({storage_id})` which only schedules our
-- own entries, never sets `flush_stopped`, and lets the rest of the queue keep
-- processing concurrently.
SYSTEM FLUSH ASYNC INSERT QUEUE t_failed_internal_async_flush;

-- Each counter should have been bumped at least once by our async flush failure.
-- Other tests running in parallel only inflate the deltas; they cannot mask a
-- regression that would leave `FailedInternalQuery` or `FailedInternalInsertQuery`
-- at zero.
WITH
    after_value AS (
        SELECT event, value FROM system.events
        WHERE event IN (
            'FailedQuery',
            'FailedInsertQuery',
            'FailedInternalQuery',
            'FailedInternalInsertQuery'
        )
    ),
    expected AS (
        SELECT 'FailedQuery' AS event UNION ALL
        SELECT 'FailedInsertQuery' UNION ALL
        SELECT 'FailedInternalQuery' UNION ALL
        SELECT 'FailedInternalInsertQuery'
    )
SELECT
    e.event AS counter,
    (ifNull(a.value, 0) - ifNull(b.value, 0)) > 0 AS bumped
FROM expected AS e
LEFT JOIN after_value AS a ON a.event = e.event
LEFT JOIN t_snap_before AS b ON b.event = e.event
ORDER BY counter;
