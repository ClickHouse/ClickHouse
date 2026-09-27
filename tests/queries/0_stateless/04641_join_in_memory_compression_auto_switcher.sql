-- Tags: no-random-settings
-- (the scenarios assert byte thresholds: compression must keep the build side below
-- `max_bytes_in_join`; randomized block-size / memory settings shift the footprint and make the
-- thresholds flaky)

-- Regression test: with `enable_join_in_memory_compression` on, `join_algorithm = 'auto'`
-- (`JoinSwitcher`) must give compression its chance before abandoning the in-memory hash join for
-- the disk-based `partial_merge`. The switcher feeds its hash join with `check_limits = false`,
-- which skips the hash join's own shrink pass, so without an explicit compression pass at the
-- switch point the decision always saw the uncompressed build size and the setting was silently
-- ineffective on the default `auto` surface.
--
-- `auto` only resolves to `JoinSwitcher` when no automatic external spilling is configured, so both
-- `auto` queries zero `max_bytes_before_external_join` / `max_bytes_ratio_before_external_join`
-- (otherwise `SpillingHashJoin` handles the memory budget - covered by 04616).

DROP TABLE IF EXISTS jimc_a_left;
DROP TABLE IF EXISTS jimc_a_right;

CREATE TABLE jimc_a_left (k UInt64, lv UInt64) ENGINE = Memory;
CREATE TABLE jimc_a_right (k UInt64, rv UInt64, pad String) ENGINE = Memory;

-- Unique keys and a highly compressible padding column on the right (build) side, ~46 MiB uncompressed.
INSERT INTO jimc_a_left SELECT number, number FROM numbers(40000);
INSERT INTO jimc_a_right SELECT number, number, repeat('x', 1000) FROM numbers(40000);

-- With compression on, the build side (~46 MiB) crosses `max_bytes_in_join` (24 MB), compresses at
-- the would-be switch point back below the limit, and the join stays in memory. The result must
-- match the plain `hash` reference and the compression event must fire.
SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_a_left AS l INNER JOIN jimc_a_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_a_left AS l INNER JOIN jimc_a_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'auto', enable_join_in_memory_compression = 1, max_bytes_in_join = 24000000,
                     max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04641_auto_on', enable_analyzer = 1;
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
SETTINGS log_comment = '04641_auto_off', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04641_auto_off' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- The build blocks that arrive after the first compression pass must be compressed too: the pass at
-- the switch point is one-shot (re-running it on the same data would only burn CPU), so it arms
-- insert-time compression in the hash join instead. Here the right side is fed in many small blocks
-- and only a few of them fit under `max_bytes_in_join`, so without that the later blocks would be
-- stored uncompressed, the next limit crossing would switch to `partial_merge`, and its own right
-- side (still uncompressed, still over the limit) would be flushed to temporary files.
SELECT (SELECT sum(cityHash64(l.number, r.k, r.pad)) FROM numbers(20000) AS l
            INNER JOIN (SELECT number AS k, repeat('x', 1000) AS pad FROM numbers(60000)) AS r ON l.number = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.number, r.k, r.pad)) FROM numbers(20000) AS l
            INNER JOIN (SELECT number AS k, repeat('x', 1000) AS pad FROM numbers(60000)) AS r ON l.number = r.k
            SETTINGS join_algorithm = 'auto', enable_join_in_memory_compression = 1, max_bytes_in_join = 12000000,
                     max_block_size = 2048, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
                     query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04641_auto_many_blocks', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] > 0, ProfileEvents['ExternalProcessingFilesTotal'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04641_auto_many_blocks' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE jimc_a_left;
DROP TABLE jimc_a_right;
