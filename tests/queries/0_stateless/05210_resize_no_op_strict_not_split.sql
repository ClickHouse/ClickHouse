-- A strict resize whose output stream count equals its input port count does
-- nothing, and Pipe::resize elides it. That check must be applied before the
-- branch that splits a resize into groups, which becomes eligible once there
-- are 2 * min_outstreams_per_resize_after_split streams (48 by default);
-- otherwise a resize that should have vanished is built as real StrictResize
-- processors and every block pays for an extra pipeline stage.
--
-- Only the plan is inspected, so the row counts below are never read.

-- The assertions below depend on the exact stream count, so `max_threads` must
-- survive as written. The free-memory limiter would otherwise silently lower it,
-- and then "no StrictResize" would hold for the wrong reason.
SET max_threads_min_free_memory_per_thread = 0;

-- Below the split threshold: no resize between the expression and the
-- aggregation. Expect 0.
SELECT count()
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM (SELECT number FROM numbers_mt(20000000) WHERE number != 0)
    SETTINGS max_threads = 47, max_block_size = 64
)
WHERE explain LIKE '%StrictResize%';

-- At and above the threshold the resize is still a no-op, so still expect 0.
-- This is the regression: before the fix a StrictResize appeared here.
SELECT count()
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM (SELECT number FROM numbers_mt(20000000) WHERE number != 0)
    SETTINGS max_threads = 48, max_block_size = 64
)
WHERE explain LIKE '%StrictResize%';

SELECT count()
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM (SELECT number FROM numbers_mt(20000000) WHERE number != 0)
    SETTINGS max_threads = 96, max_block_size = 64
)
WHERE explain LIKE '%StrictResize%';

-- Guard: the case above is only meaningful if the pipeline really is that wide,
-- since a narrower one would have no eligible split at all. Expect 1.
SELECT count() >= 1
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM (SELECT number FROM numbers_mt(20000000) WHERE number != 0)
    SETTINGS max_threads = 48, max_block_size = 64
)
WHERE explain LIKE '%× 48%';

-- A resize that genuinely changes the stream count must still be split, so the
-- fix above must not have disabled the optimisation. The split form is printed
-- as "Resize x N ..." rather than a single "Resize N -> M". Expect 1.
SELECT count() >= 1
FROM (
    EXPLAIN PIPELINE
    SELECT number % 1000 AS k, count() FROM numbers_mt(20000000) GROUP BY k
    SETTINGS max_threads = 64, group_by_two_level_threshold = 1
)
WHERE explain LIKE '%Resize × %';
