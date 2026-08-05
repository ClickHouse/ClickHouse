-- Tags: no-random-settings
-- (the scenario relies on an exact right-side block layout and on the left side being probed in
-- large blocks: randomized block-size / memory settings shift both and make it meaningless)

-- Regression test for the probe-side decompression order of `enable_join_in_memory_compression`.
--
-- The build side is three stored blocks of ~40 MB decompressed each, and the probe rows alternate
-- between them on every row (`k` strides across the three inserts). Any two of the blocks exceed
-- the 64 MiB decompressed working-set budget together, so copying the output in row order would
-- release and re-decompress tens of megabytes on every block switch - one release per output row,
-- turning a sub-second join into one that runs for hours (observed as a stress-test "hung" query).
-- The output is instead copied grouped by stored block: each distinct block is decompressed once
-- per output batch, so the number of budget-forced releases stays tiny whatever order the probe
-- rows reference the blocks in.

DROP TABLE IF EXISTS jimc_alt_left;
DROP TABLE IF EXISTS jimc_alt_right;

CREATE TABLE jimc_alt_left (k UInt64) ENGINE = Memory;
-- Row i probes build row (i % 3) * 27000 + i / 3: consecutive probe rows land in different
-- stored blocks.
INSERT INTO jimc_alt_left SELECT (number % 3) * 27000 + intDiv(number, 3) FROM numbers(81000);

CREATE TABLE jimc_alt_right (k UInt64, pad String) ENGINE = Memory;
-- Three inserts of 27000 rows * ~1.5 KB = three stored blocks of ~40 MB decompressed.
INSERT INTO jimc_alt_right SELECT number, repeat('x', 1500) FROM numbers(27000);
INSERT INTO jimc_alt_right SELECT 27000 + number, repeat('y', 1500) FROM numbers(27000);
INSERT INTO jimc_alt_right SELECT 54000 + number, repeat('z', 1500) FROM numbers(27000);

-- The compressed run must produce the same result as the uncompressed one.
SELECT (SELECT sum(cityHash64(l.k, r.pad))
        FROM jimc_alt_left AS l
        INNER JOIN jimc_alt_right AS r ON l.k = r.k
        SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0)
     = (SELECT sum(cityHash64(l.k, r.pad))
        FROM jimc_alt_left AS l
        INNER JOIN jimc_alt_right AS r ON l.k = r.k
        SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1,
                 max_bytes_in_join = 200000000)
SETTINGS log_comment = '04759_alt', enable_analyzer = 1;

SYSTEM FLUSH LOGS query_log;

-- Compression fired during the build, and the probe stayed block-grouped: a handful of
-- budget-forced releases at most (one per ~64 MiB of decompressed data per output batch), not one
-- per block switch (which would be tens of thousands here).
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] > 0,
       ProfileEvents['JoinInMemoryDecompressWorkingSetReleases'] BETWEEN 1 AND 100
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04759_alt' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE jimc_alt_left;
DROP TABLE jimc_alt_right;
