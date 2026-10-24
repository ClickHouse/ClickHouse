-- Key types across the spilling `LIMIT N BY`.
--
-- Spilling writes the grouping state out as ordinary rows, so every key type has to survive a round
-- trip out of the hash table and back. The types below each take a different route: `NULL` keys are
-- kept outside the hash table cells, `LowCardinality` keys go through the dictionary-aware method,
-- and a pair of fixed-size keys is packed into one cell whose parts come back in the method's own
-- column order rather than the query's. A type that is restored wrongly does not throw - it produces
-- a group that no longer matches the rows it was counting.
--
-- Every key value appears exactly 3 times, so `LIMIT 2 BY` must return exactly 2 rows for every one
-- of the 20000 groups, whichever 2 rows the spill happens to pick.

SET max_threads = 4;
SET max_block_size = 8192;
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_bytes_before_external_limit_by = 1;

SELECT 'String key';
SELECT min(c), max(c), count()
FROM
(
    SELECT k, count() AS c
    FROM
    (
        SELECT toString(number % 20000) AS k, number AS v
        FROM numbers_mt(60000)
        LIMIT 2 BY k
    )
    GROUP BY k
);

SELECT 'LowCardinality(String) key';
SELECT min(c), max(c), count()
FROM
(
    SELECT k, count() AS c
    FROM
    (
        SELECT toLowCardinality(toString(number % 20000)) AS k, number AS v
        FROM numbers_mt(60000)
        LIMIT 2 BY k
    )
    GROUP BY k
);

-- One key value in 20000 is NULL, so the group that lives outside the hash table cells is present
-- in every spilled state and has to come back as a group of its own.
SELECT 'Nullable(String) key, including the NULL group';
SELECT min(c), max(c), count(), countIf(k IS NULL)
FROM
(
    SELECT k, count() AS c
    FROM
    (
        SELECT if(number % 20000 = 0, NULL, toString(number % 20000)) AS k, number AS v
        FROM numbers_mt(60000)
        LIMIT 2 BY k
    )
    GROUP BY k
) SETTINGS log_comment = '05142_nullable_key';

SELECT 'LowCardinality(Nullable(String)) key, including the NULL group';
SELECT min(c), max(c), count(), countIf(k IS NULL)
FROM
(
    SELECT k, count() AS c
    FROM
    (
        SELECT toLowCardinality(if(number % 20000 = 0, NULL, toString(number % 20000))) AS k, number AS v
        FROM numbers_mt(60000)
        LIMIT 2 BY k
    )
    GROUP BY k
);

-- Two fixed-size keys of different widths are packed into a single hash table key, and the method
-- reads the parts back in its own order, so a mismatch here pairs `k1` with the wrong `k2`.
SELECT 'two fixed-size keys packed into one hash key';
SELECT min(c), max(c), count(), countIf(k2 != k1 + 1)
FROM
(
    SELECT k1, k2, count() AS c
    FROM
    (
        SELECT toUInt32(number % 20000) AS k1, toUInt64(number % 20000) + 1 AS k2, number AS v
        FROM numbers_mt(60000)
        LIMIT 2 BY k1, k2
    )
    GROUP BY k1, k2
);

SELECT 'mixed String and numeric keys';
SELECT min(c), max(c), count(), countIf(toString(k2) != k1)
FROM
(
    SELECT k1, k2, count() AS c
    FROM
    (
        SELECT toString(number % 20000) AS k1, toUInt64(number % 20000) AS k2, number AS v
        FROM numbers_mt(60000)
        LIMIT 2 BY k1, k2
    )
    GROUP BY k1, k2
);

-- A constant key is one single group, which the transform tracks with a plain counter instead of a
-- hash table. There is no state to spill, so this checks that the spilling transform still answers
-- correctly when its spill can never trigger.
SELECT 'constant key is a single group';
SELECT count(), uniqExact(v)
FROM
(
    SELECT 'c' AS k, number AS v
    FROM numbers_mt(60000)
    LIMIT 2 BY k
);

-- Without this guard the `NULL` group assertions above would still pass if nothing ever spilled.
SYSTEM FLUSH LOGS query_log;

SELECT 'the spill actually happened';
SELECT sum(ProfileEvents['ExternalLimitByWritePart']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
    AND log_comment = '05142_nullable_key';
