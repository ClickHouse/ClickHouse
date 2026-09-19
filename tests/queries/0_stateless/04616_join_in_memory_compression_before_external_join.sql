-- Tags: no-random-settings
-- (the scenarios assert byte thresholds: compression must keep the build side below half of
-- `max_bytes_before_external_join`; randomized block-size / memory settings shift the footprint
-- and make the thresholds flaky)

-- Regression test: with `enable_join_in_memory_compression` on, compression must act as the
-- intermediate step before spilling also when the *only* configured memory budget is
-- `max_bytes_before_external_join` (`max_bytes_ratio_before_external_join` folds into the same
-- effective threshold). `SpillingHashJoin` switches to `GraceHashJoin` once the stored size reaches
-- half of that threshold; a compressible build side must compress at that point and stay in memory
-- instead of spilling.

DROP TABLE IF EXISTS jimc_e_left;
DROP TABLE IF EXISTS jimc_e_right;

CREATE TABLE jimc_e_left (k UInt64, lv UInt64) ENGINE = Memory;
CREATE TABLE jimc_e_right (k UInt64, rv UInt64, pad String) ENGINE = Memory;

-- Unique keys and a highly compressible padding column on the right (build) side, ~46 MiB uncompressed.
INSERT INTO jimc_e_left SELECT number, number FROM numbers(40000);
INSERT INTO jimc_e_right SELECT number, number, repeat('x', 1000) FROM numbers(40000);

-- Control: with compression off and only the external threshold set, the build side (~46 MiB) crosses
-- half of the 24 MB threshold and the join spills (switches to `GraceHashJoin`).
SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) > 0 FROM jimc_e_left AS l INNER JOIN jimc_e_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0,
         max_bytes_before_external_join = 24000000, query_plan_join_swap_table = 'false',
         log_comment = '04616_hash_off', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04616_hash_off' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- With compression on, the stored blocks compress before the switch check fires, the (tiny) compressed
-- build side stays below the threshold, and the join never switches to `GraceHashJoin`.
SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_e_left AS l INNER JOIN jimc_e_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_before_external_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_e_left AS l INNER JOIN jimc_e_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1, max_bytes_in_join = 0, max_bytes_before_external_join = 24000000, query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04616_hash_on', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'], ProfileEvents['JoinInMemoryCompressedColumns'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04616_hash_on' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- Same for `parallel_hash` (the threshold is evaluated against the join's total across all slots).
SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_e_left AS l INNER JOIN jimc_e_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_join_in_memory_compression = 0, max_bytes_before_external_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_e_left AS l INNER JOIN jimc_e_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_join_in_memory_compression = 1, max_bytes_in_join = 0, max_bytes_before_external_join = 24000000, query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04616_parallel_hash_on', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'], ProfileEvents['JoinInMemoryCompressedColumns'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04616_parallel_hash_on' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE jimc_e_left;
DROP TABLE jimc_e_right;
