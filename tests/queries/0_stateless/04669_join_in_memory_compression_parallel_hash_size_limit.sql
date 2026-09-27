-- Tags: no-random-settings
-- (the scenario relies on the right-side block layout, the number of insert threads and the exact
-- `max_bytes_in_join` margin: randomized block-size / thread / memory settings shift all of them)

-- Regression test for the `parallel_hash` last-chance compression pass before the global
-- `max_bytes_in_join` check.
--
-- Concurrent build-side inserts compare the half-of-`max_bytes_in_join` compression trigger
-- against a global byte counter that compensates only their own slot's unpublished delta. One wave
-- of inserts on different slots can therefore jump the logical join from below the trigger straight
-- over the full limit with no slot compressing, and the final global size check used to throw
-- `SET_SIZE_LIMIT_EXCEEDED` before compression ever got a chance. Now the failure path runs one
-- forced compression pass over all slots first (the same last chance `SpillingHashJoin` gives the
-- build side before spilling), so a compressible build side must fit and the query must succeed
-- regardless of insert timing.
--
-- The right side is ~100 MB decompressed (highly compressible padding) inserted in many blocks and
-- read with many threads, against a 60 MB limit: without compression the limit is exceeded
-- deterministically; with compression the build side shrinks to a few MB.

DROP TABLE IF EXISTS jimc_sl_left;
DROP TABLE IF EXISTS jimc_sl_right;

CREATE TABLE jimc_sl_left (k UInt64) ENGINE = Memory;
INSERT INTO jimc_sl_left SELECT number FROM numbers(65536);

CREATE TABLE jimc_sl_right (k UInt64, pad String) ENGINE = Memory;
INSERT INTO jimc_sl_right SELECT number, repeat('x', 1500) FROM numbers(65536) SETTINGS max_block_size = 256;

-- Negative control: without compression the build side cannot fit under the limit.
SELECT sum(cityHash64(l.k, r.pad))
FROM jimc_sl_left AS l
INNER JOIN jimc_sl_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'parallel_hash', enable_join_in_memory_compression = 0,
         max_bytes_in_join = 60000000, max_threads = 16, enable_analyzer = 1
FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- With compression the query must succeed and produce the same result as the unconstrained run.
SELECT (SELECT sum(cityHash64(l.k, r.pad))
        FROM jimc_sl_left AS l
        INNER JOIN jimc_sl_right AS r ON l.k = r.k
        SETTINGS join_algorithm = 'parallel_hash', enable_join_in_memory_compression = 0)
     = (SELECT sum(cityHash64(l.k, r.pad))
        FROM jimc_sl_left AS l
        INNER JOIN jimc_sl_right AS r ON l.k = r.k
        SETTINGS join_algorithm = 'parallel_hash', enable_join_in_memory_compression = 1,
                 max_bytes_in_join = 60000000, max_threads = 16)
SETTINGS log_comment = '04669_sl', enable_analyzer = 1;

SYSTEM FLUSH LOGS query_log;

-- Compression fired during the build (at the trigger or in the last-chance pass).
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04669_sl' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE jimc_sl_left;
DROP TABLE jimc_sl_right;
