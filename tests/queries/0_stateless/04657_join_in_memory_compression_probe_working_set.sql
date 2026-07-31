-- Tags: no-random-settings
-- (the scenario relies on an exact right-side block layout and on the left side being probed in
-- large blocks: randomized block-size / memory settings shift both and make it meaningless)

-- Regression test for the probe-side working set of `enable_join_in_memory_compression`.
--
-- One probe output batch decompresses the distinct stored blocks it references. When the build side
-- consists of many tiny blocks and each probe row matches a different one, a batch used to pin one
-- fully decompressed block per match until the whole batch was materialized, re-materializing an
-- unbounded part of the build side (up to `max_block_size` blocks at once) - exactly what the
-- setting is supposed to avoid. Now the decompressed working set held at once is bounded: blocks
-- already copied into the output are released early and decompressed anew if referenced again.
--
-- The right side is a Memory table filled with `max_block_size = 4`, so the join build receives
-- 16384 blocks of 4 rows (~1.5 KB per row, ~100 MB decompressed in total, compressing to almost
-- nothing). The left side probes in default-sized blocks, so a single output batch references
-- thousands of distinct stored blocks (~100 MB decompressed) - well past the 64 MiB working-set
-- budget - and must release mid-batch, observable as `JoinInMemoryDecompressWorkingSetReleases`.

DROP TABLE IF EXISTS jimc_ws_left;
DROP TABLE IF EXISTS jimc_ws_right;

CREATE TABLE jimc_ws_left (k UInt64) ENGINE = Memory;
INSERT INTO jimc_ws_left SELECT number FROM numbers(65536);

CREATE TABLE jimc_ws_right (k UInt64, pad String) ENGINE = Memory;
INSERT INTO jimc_ws_right SELECT number, repeat('x', 1500) FROM numbers(65536) SETTINGS max_block_size = 4;

-- The compressed run must produce the same result as the uncompressed one.
SELECT (SELECT sum(cityHash64(l.k, r.pad))
        FROM jimc_ws_left AS l
        INNER JOIN jimc_ws_right AS r ON l.k = r.k
        SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0)
     = (SELECT sum(cityHash64(l.k, r.pad))
        FROM jimc_ws_left AS l
        INNER JOIN jimc_ws_right AS r ON l.k = r.k
        SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1,
                 max_bytes_in_join = 50000000)
SETTINGS log_comment = '04657_ws', enable_analyzer = 1;

SYSTEM FLUSH LOGS query_log;

-- Compression fired during the build, and the probe hit the decompressed working-set budget at
-- least once (so it did not keep one decompressed block per match alive until the batch ended).
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] > 0,
       ProfileEvents['JoinInMemoryDecompressWorkingSetReleases'] >= 1
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04657_ws' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE jimc_ws_left;
DROP TABLE jimc_ws_right;
