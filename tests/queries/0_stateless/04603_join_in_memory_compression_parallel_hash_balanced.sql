-- Tags: no-random-settings
-- (the scenario asserts that compression keeps the join under `max_bytes_in_join`; randomized
-- block-size / memory settings shift the per-slot footprint and make the threshold flaky)
--
-- `enable_analyzer = 1` is pinned like in 04494: `query_plan_join_swap_table = 'false'` (which forces
-- the right, compressible table as the build side) is an analyzer/query-plan-only setting.

-- Regression test for the `parallel_hash` compression trigger with balanced slots. `max_bytes_in_join`
-- is enforced on the logical join's total, but each internal slot used to evaluate the compression
-- trigger against its own slot-local byte count. With balanced keys, every slot stays under its own
-- half-of-`max_bytes_in_join` threshold while the global total already exceeds the limit, so the query
-- used to throw `SET_SIZE_LIMIT_EXCEEDED` before any slot compressed. The trigger now fires on the
-- logical join's total.
--
-- The numbers below pin that shape: the right side is ~32 MiB raw across 4 slots (~8 MiB per slot),
-- and `max_bytes_in_join = 25000000` puts the half-threshold at ~12.5 MiB - above any slot-local
-- count, below the raw total. The right side is inserted in ~8192-row blocks so the build side grows
-- incrementally and the trigger has room to fire before the limit check.

DROP TABLE IF EXISTS jimc_b_left;
DROP TABLE IF EXISTS jimc_b_right;

CREATE TABLE jimc_b_left (k UInt64, lv UInt64) ENGINE = Memory;
CREATE TABLE jimc_b_right (k UInt64, rv UInt64, pad String) ENGINE = Memory;

INSERT INTO jimc_b_left SELECT number, number FROM numbers(50000);
INSERT INTO jimc_b_right SELECT number, number, repeat('x', 600) FROM numbers(50000)
    SETTINGS max_block_size = 8192, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;

SELECT (SELECT sum(cityHash64(l.k, r.rv, r.pad)) FROM jimc_b_left AS l INNER JOIN jimc_b_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_join_in_memory_compression = 0)
     = (SELECT sum(cityHash64(l.k, r.rv, r.pad)) FROM jimc_b_left AS l INNER JOIN jimc_b_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_join_in_memory_compression = 1, max_bytes_in_join = 25000000, query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04603_parallel_hash_balanced', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04603_parallel_hash_balanced' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE jimc_b_left;
DROP TABLE jimc_b_right;
