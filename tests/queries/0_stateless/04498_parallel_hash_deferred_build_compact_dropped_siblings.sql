-- Tags: no-random-settings, no-random-merge-tree-settings

-- The deferred (exact-size) parallel_hash build drops a per-slot block with no kept row (see
-- 04414_parallel_hash_deferred_build_skip_discarded_blocks). The kept sibling blocks of the same
-- source block must then be compacted (materialized to their insertable rows): under the zero-copy
-- scatter all the per-slot blocks COW-share the one source block, so dropping a sibling frees nothing
-- while the kept blocks' row-proportional `ScatteredBlock::allocatedBytes` shares stop summing to the
-- resident whole - `buffered_bytes`, `getTotalByteCount` and `getProjectedTotalByteCount` under-counted
-- the retained bytes (and the row-ratio share of the key bytes under-counted kept rows with
-- longer-than-average `String` keys), so the early `max_bytes_in_join` guard and the
-- `max_bytes_before_external_join` spill checks could fire late. After compaction everything buffered
-- owns exactly its rows, so the counted bytes equal the retained bytes.
-- Related: https://github.com/ClickHouse/ClickHouse/pull/108129#discussion_r3510581031

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET parallel_hash_join_threshold = 0;          -- force ConcurrentHashJoin regardless of build size
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';      -- keep the big table as the build (right) side
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;              -- single-map build path; no distributed read
-- The peak-memory assertion below must not depend on the storage flavor: on object-storage runs the
-- default threadpool reader with prefetching adds ~17 MB of query-tracked read buffers on top of the
-- ~30 MB local-disk peak. Pin the plain remote read path so the buffered-read term stays per-stream.
SET remote_filesystem_read_method = 'read';
SET remote_filesystem_read_prefetch = 0;
SET allow_prefetched_read_pool_for_remote_filesystem = 0;

DROP TABLE IF EXISTS t_compact_build;
DROP TABLE IF EXISTS t_compact_probe;
-- String keys: forces the zero-copy scatter (shared source block) and the `two_level_key_string` maps
-- whose replay arena-copies the key bytes (`buffered_key_bytes` tracking).
CREATE TABLE t_compact_build (k String, keep UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_compact_probe (k String, v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- ~4 kept rows per 64K source block (number % 16384 = 0), so nearly every block scatters its few kept
-- rows over only some of the 4 slots: the other slots' blocks are dropped and the kept siblings must be
-- compacted. The kept rows carry 1024-byte keys (longer than the block average, the under-counted case);
-- the rejected rows pad the block to ~50 MB total so the uncompacted right side cannot fit the byte cap.
INSERT INTO t_compact_build
    SELECT if(number % 16384 = 0, concat('key_', toString(number), repeat('x', 1024)), concat('reject_', toString(number), repeat('y', 32))),
           number % 16384 = 0,
           number * 3
    FROM numbers(1000000);
-- Only the rows that can match carry the long key format; the rest stay short to keep the probe cheap.
INSERT INTO t_compact_probe
    SELECT if(number % 16384 = 0, concat('key_', toString(number), repeat('x', 1024)), concat('probe_', toString(number))),
           number * 7
    FROM numbers(1000000);

-- The streaming `hash` control runs UNCAPPED: unlike 04414 (contiguous keep run, so whole blocks pop),
-- here every ~64K source block holds a few kept rows, so the streaming build keeps every block (~50 MB)
-- - it provides the reference result only.
SELECT 'compact_drop', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_compact_probe l ANY LEFT JOIN t_compact_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'hash';

-- max_bytes_in_join = 32 MB sits well above the empty per-slot maps' baseline plus the ~62 compacted
-- kept rows (~70 KiB). Before the fix this query ALSO passed the cap - that is the bug: the kept
-- per-slot blocks pinned every ~60 MB source block via the shared selector while their
-- row-proportional charge stayed under the cap, so the real peak was ~61 MiB against a 32 MB limit.
-- The peak-memory assertion below pins the fix (counted == retained), and the result must AGREE with
-- the streaming `hash` reference above.
SELECT 'compact_drop', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_compact_probe l ANY LEFT JOIN t_compact_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'parallel_hash', join_overflow_mode = 'throw', max_bytes_in_join = 32000000,
         log_comment = '04498_compact_dropped_siblings';

-- NULL keys are kept (the streaming pop's seeding rule) but never inserted, so a kept block may compact
-- to zero insertable rows and must then be dropped like a popped zero-insert block, not buffered empty.
-- Also covers compaction with a Nullable(String) key null map. Correctness only - no byte cap.
DROP TABLE IF EXISTS t_compact_null_build;
CREATE TABLE t_compact_null_build (k Nullable(String), keep UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_compact_null_build
    SELECT if(number % 3 = 0, NULL, concat('key_', toString(number), repeat('x', 1024))),
           number % 16384 = 0,
           number * 3
    FROM numbers(1000000);

SELECT 'compact_null', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_compact_probe l ANY LEFT JOIN t_compact_null_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'hash';

SELECT 'compact_null', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_compact_probe l ANY LEFT JOIN t_compact_null_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'parallel_hash';

-- Positive controls for the byte-capped parallel_hash query:
-- 1. It used the deferred exact-size build, and the reserve is sized by the inserted (keep = 1) keys
--    (~62), not by all 1M build rows - confirming the deferred path engaged and the dropped/compacted
--    blocks stayed out of the sizing.
-- 2. Its real peak memory respects the byte cap it ran under. Before the fix the row-proportional
--    charge of the kept per-slot blocks under-counted the pinned source blocks, so the query passed
--    the 32 MB cap while actually peaking at ~61 MiB; with compaction it peaks at ~26 MiB (~30 MB
--    query-wide, a few MB more with the pinned remote read path or coverage instrumentation). The
--    52 MB bound keeps >= 12 MB of margin below the broken ~64 MB level while staying clear of the
--    fixed peak on every storage/build flavor.
SYSTEM FLUSH LOGS query_log;
SELECT 'deferred build engaged',
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = count(),
    max(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables']) BETWEEN 1 AND 10000,
    max(memory_usage) < 52000000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment = '04498_compact_dropped_siblings';

DROP TABLE t_compact_build;
DROP TABLE t_compact_probe;
DROP TABLE t_compact_null_build;
