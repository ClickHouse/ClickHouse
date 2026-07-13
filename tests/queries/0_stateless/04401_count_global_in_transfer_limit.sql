-- Tags: shard
-- shard: Uses `remote('127.0.0.2', ...)`, so it must be skipped where the server cannot
--        listen on `127.0.0.2` (e.g. the macOS `arm_darwin` fast-test build).
--
-- Regression test: `count()` over a distributed `MergeTree` table with a `GLOBAL IN`
-- predicate must not raise a `Not-ready Set is passed ...` `LOGICAL_ERROR` when
-- `max_rows_to_transfer` is set.
--
-- With a transfer limit active, the in-place `GLOBAL IN` set build defers to the
-- streaming path (so that the limit is enforced on the full payload), leaving the
-- set not ready. The synchronous `count()` optimizations -- the min-max-count
-- projection (`getMinMaxCountProjectionBlock`) and trivial count
-- (`totalRowsByPartitionPredicate`) -- have no streaming fallback, so they must
-- decline and fall back to a normal read instead of evaluating `IN` against a
-- not-ready set.

DROP TABLE IF EXISTS t_count_global_in;

CREATE TABLE t_count_global_in (x Int64) ENGINE = MergeTree ORDER BY x AS SELECT * FROM numbers(10);

SET max_rows_to_transfer = 1000000000;

-- Old analyzer, min-max-count projection path (default settings).
SELECT count() FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), t_count_global_in)
WHERE 'XXX' GLOBAL IN (SELECT 'XXX') SETTINGS enable_analyzer = 0;

-- Old analyzer, trivial-count path (projections disabled).
SELECT count() FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), t_count_global_in)
WHERE 'XXX' GLOBAL IN (SELECT 'XXX')
SETTINGS enable_analyzer = 0, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

-- New analyzer (sanity).
SELECT count() FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), t_count_global_in)
WHERE 'XXX' GLOBAL IN (SELECT 'XXX') SETTINGS enable_analyzer = 1;

DROP TABLE t_count_global_in;
