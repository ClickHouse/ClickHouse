-- Tags: no-random-settings
-- (the scenarios assert byte thresholds: compression must keep the build side below
-- `max_bytes_in_join`; randomized block-size / memory settings shift the footprint and make the
-- thresholds flaky)

-- With `enable_join_in_memory_compression` on, `join_algorithm = 'auto'` (`JoinSwitcher`) sees the
-- compressed size of the build side, so a compressible build side stays in the in-memory hash join
-- instead of switching to the disk-based `partial_merge`.
--
-- `auto` only resolves to `JoinSwitcher` when no automatic external spilling is configured, so both
-- `auto` queries zero `max_bytes_before_external_join` / `max_bytes_ratio_before_external_join`
-- (otherwise `SpillingHashJoin` handles the memory budget - covered by 04616).

-- Compress from the first stored block.
SET join_in_memory_compression_min_bytes = 0;

DROP TABLE IF EXISTS jimc_a_left;
DROP TABLE IF EXISTS jimc_a_right;

CREATE TABLE jimc_a_left (k UInt64, lv UInt64) ENGINE = Memory;
CREATE TABLE jimc_a_right (k UInt64, rv UInt64, pad String) ENGINE = Memory;

-- Unique keys and a highly compressible padding column on the right (build) side, ~46 MiB uncompressed.
INSERT INTO jimc_a_left SELECT number, number FROM numbers(40000);
INSERT INTO jimc_a_right SELECT number, number, repeat('x', 1000) FROM numbers(40000);

-- With compression on, the build side (~46 MiB uncompressed) stays below `max_bytes_in_join` (24 MB),
-- and the join stays in memory. The result must match the plain `hash` reference and the compression
-- event must fire.
SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_a_left AS l INNER JOIN jimc_a_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_a_left AS l INNER JOIN jimc_a_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'auto', enable_join_in_memory_compression = 1, max_bytes_in_join = 24000000,
                     max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04641_auto_on';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04641_auto_on' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- Control: with compression off the same query switches to `partial_merge` and still returns the
-- correct result, and nothing is compressed.
SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_a_left AS l INNER JOIN jimc_a_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_a_left AS l INNER JOIN jimc_a_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'auto', enable_join_in_memory_compression = 0, max_bytes_in_join = 24000000,
                     max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04641_auto_off';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04641_auto_off' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- The right side is fed in many small blocks, and only a few of them fit under `max_bytes_in_join`
-- uncompressed: every block must be compressed on insertion, otherwise the join would switch to
-- `partial_merge`, and its right side would be flushed to temporary files.
SELECT (SELECT sum(cityHash64(l.number, r.k, r.pad)) FROM numbers(20000) AS l
            INNER JOIN (SELECT number AS k, repeat('x', 1000) AS pad FROM numbers(60000)) AS r ON l.number = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.number, r.k, r.pad)) FROM numbers(20000) AS l
            INNER JOIN (SELECT number AS k, repeat('x', 1000) AS pad FROM numbers(60000)) AS r ON l.number = r.k
            SETTINGS join_algorithm = 'auto', enable_join_in_memory_compression = 1, max_bytes_in_join = 12000000,
                     max_block_size = 2048, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
                     query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04641_auto_many_blocks';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] > 0, ProfileEvents['ExternalProcessingFilesTotal'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04641_auto_many_blocks' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE jimc_a_left;
DROP TABLE jimc_a_right;
