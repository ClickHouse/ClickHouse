-- Tags: no-random-settings
-- (the `parallel_hash` case asserts that compression keeps the join under `max_bytes_in_join`; randomized
-- block-size / memory settings shift the per-slot footprint and make that threshold flaky)
--
-- `enable_analyzer = 1` is pinned on both outer queries (like 04493): the scenarios use
-- `query_plan_join_swap_table = 'false'` to force the right (compressible) table as the build side, and
-- that is an analyzer/query-plan-only setting. Under the old analyzer it is a no-op, so the build-side
-- choice (and therefore the per-slot footprint) is undefined and the tight `max_bytes_in_join` limit can
-- be exceeded before compression catches up (observed `SET_SIZE_LIMIT_EXCEEDED` only on the old-analyzer
-- run, while every new-analyzer config passes). The old-analyzer compression path itself is still covered
-- by 04492, which is not analyzer-pinned.

-- Regression tests for the documented contract of `enable_join_in_memory_compression` beyond plain
-- `hash`: the setting must compress the stored right-side blocks under memory pressure for
-- `parallel_hash` (slots are merged into slot 0 via two-level maps) and standalone `grace_hash` (the
-- active in-memory bucket). Each scenario checks the compressed result equals the uncompressed one and,
-- via the `JoinInMemoryCompressedColumns` profile event, that compression really happened.
--
-- Per the documented contract, standalone `grace_hash` compression is a one-time compaction of the
-- active in-memory bucket when it reaches `max_bytes_in_join`, just before the bucket would rehash or
-- spill. Two grace-specific aspects are deliberately out of the contract and so are not asserted here:
--   * the `max_memory_usage` trigger does not apply to a `grace_hash` bucket (the bucket only reaches
--     this hook through the join `SizeLimits` overflow check, not the query memory tracker); and
--   * blocks inserted into the bucket after the one-time compaction are not compressed again.
-- For `hash`/`parallel_hash` the `max_memory_usage` trigger is wired, but is not asserted either: its
-- firing depends on total query-memory growth, which in a small test is dominated by the uncompressible
-- hash table rather than the compressible stored blocks, so there is no robust threshold to assert.

DROP TABLE IF EXISTS jimc_c_left;
DROP TABLE IF EXISTS jimc_c_right;

CREATE TABLE jimc_c_left (k UInt64, lv UInt64) ENGINE = Memory;
CREATE TABLE jimc_c_right (k UInt64, rv UInt64, pad String) ENGINE = Memory;

INSERT INTO jimc_c_left SELECT number, number FROM numbers(50000);
INSERT INTO jimc_c_right SELECT number, number, repeat('x', 600) FROM numbers(50000);

-- 1. parallel_hash: each slot compresses independently, then the slots are merged into slot 0 (two-level
-- maps). Without propagating have_compressed into slot 0 the probe reads ColumnCompressed and throws.
SELECT (SELECT sum(cityHash64(l.k, r.rv, r.pad)) FROM jimc_c_left AS l INNER JOIN jimc_c_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_join_in_memory_compression = 0)
     = (SELECT sum(cityHash64(l.k, r.rv, r.pad)) FROM jimc_c_left AS l INNER JOIN jimc_c_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_join_in_memory_compression = 1, max_bytes_in_join = 12000000, query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04494_parallel_hash', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04494_parallel_hash' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- 2. standalone grace_hash: the active in-memory bucket must compress under pressure instead of only
-- spilling/rehashing. Without compressing the bucket before rehash the event stays zero.
SELECT (SELECT sum(cityHash64(l.k, r.rv, r.pad)) FROM jimc_c_left AS l INNER JOIN jimc_c_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 1, enable_join_in_memory_compression = 0)
     = (SELECT sum(cityHash64(l.k, r.rv, r.pad)) FROM jimc_c_left AS l INNER JOIN jimc_c_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 1, enable_join_in_memory_compression = 1, max_bytes_in_join = 8000000, query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04494_grace_hash', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinInMemoryCompressedColumns'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04494_grace_hash' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE jimc_c_left;
DROP TABLE jimc_c_right;
