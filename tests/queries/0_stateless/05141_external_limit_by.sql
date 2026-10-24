-- Correctness of the hash-based `LIMIT N BY` when it spills its grouping state to disk.
--
-- Spilling is where `LIMIT BY` can silently lose or duplicate rows: the hash table is written out
-- as a run and started over many times, so the rows of one group end up scattered over several
-- runs and only meet again in the final merge. If the merge forgot how many rows a group had
-- already contributed to an earlier run, a group would come back with too few or too many rows,
-- which still looks like a perfectly plausible `LIMIT BY` answer.
--
-- The input is built so that the answer does not depend on which rows the transform happens to
-- see first: every key appears exactly 3 times, so `LIMIT 3 BY` must return the whole input and
-- `LIMIT 2 BY` must return exactly 2 rows per key whichever 2 they are. That matters because
-- spilling is only used when the output order is free anyway (more than one input stream).

-- CI randomizes these; pin them so the spill is the only thing under test. Spilling is only chosen
-- when `LIMIT BY` has more than one input stream, which is what `max_threads` guarantees here, and
-- a small block size keeps that true while also giving the merge many runs to reconcile.
SET max_threads = 4;
SET max_block_size = 8192;
-- A state threshold of one byte makes every chunk spill, so the rows of a group are spread over
-- dozens of runs and the final merge has to put every one of them back together.
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_bytes_before_external_limit_by = 1;

SELECT 'LIMIT 3 BY: every row of a 3-row group survives';
SELECT count(), uniqExact(v), min(v), max(v)
FROM
(
    SELECT number % 100000 AS k, number AS v
    FROM numbers_mt(300000)
    LIMIT 3 BY k
);

SELECT 'LIMIT 5 BY: a limit above the group size keeps the group whole';
SELECT count(), uniqExact(v)
FROM
(
    SELECT number % 100000 AS k, number AS v
    FROM numbers_mt(300000)
    LIMIT 5 BY k
);

SELECT 'LIMIT 2 BY: exactly two rows per key, and each row belongs to its key';
SELECT count(), uniqExact(k), uniqExact(v), countIf(v % 100000 != k)
FROM
(
    SELECT number % 100000 AS k, number AS v
    FROM numbers_mt(300000)
    LIMIT 2 BY k
) SETTINGS log_comment = '05141_limit_2_by';

SELECT 'LIMIT 2 BY: no group comes back short or long';
SELECT min(c), max(c), count()
FROM
(
    SELECT k, count() AS c
    FROM
    (
        SELECT number % 100000 AS k, number AS v
        FROM numbers_mt(300000)
        LIMIT 2 BY k
    )
    GROUP BY k
);

SELECT 'LIMIT 2 OFFSET 1 BY: the offset skips one row per group, not one row overall';
SELECT min(c), max(c), count(), sum(distinct_rows)
FROM
(
    SELECT k, count() AS c, uniqExact(v) AS distinct_rows
    FROM
    (
        SELECT number % 100000 AS k, number AS v
        FROM numbers_mt(300000)
        LIMIT 2 OFFSET 1 BY k
    )
    GROUP BY k
);

SELECT 'LIMIT 1 BY: one row per key';
SELECT count(), uniqExact(k), countIf(v % 100000 != k)
FROM
(
    SELECT number % 100000 AS k, number AS v
    FROM numbers_mt(300000)
    LIMIT 1 BY k
);

-- `LIMIT 3 BY` returns the whole input, so unlike the partial limits above its answer is fully
-- determined and can be compared row for row - against the input itself, which needs no `LIMIT BY`
-- to produce and therefore cannot be affected by the settings under test. Together with the row
-- count asserted above, an empty difference means the two sides are equal.
SELECT 'the spilled LIMIT 3 BY result is exactly the input';
SELECT count()
FROM
(
    (
        SELECT number % 100000 AS k, number AS v
        FROM numbers_mt(300000)
        LIMIT 3 BY k
    )
    EXCEPT
    SELECT number % 100000 AS k, number AS v FROM numbers(300000)
);

-- Without this guard every assertion above would still pass if the transform never spilled,
-- i.e. if the test had quietly stopped testing anything.
SYSTEM FLUSH LOGS query_log;

SELECT 'the spill actually happened';
SELECT
    sum(ProfileEvents['ExternalLimitByWritePart']) > 0 AS wrote_runs,
    sum(ProfileEvents['ExternalLimitByMerge']) > 0 AS merged_runs,
    sum(ProfileEvents['ExternalLimitByUncompressedBytes']) > 0 AS wrote_bytes
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
    AND log_comment = '05141_limit_2_by';
