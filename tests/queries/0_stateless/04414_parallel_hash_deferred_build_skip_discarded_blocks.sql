-- Tags: no-random-settings, no-random-merge-tree-settings

-- The deferred (exact-size) parallel_hash build must not throw SET_SIZE_LIMIT_EXCEEDED on right
-- blocks the streaming `hash` build discards. For a one-row-per-key (MapsOne: ANY / SEMI / ANTI)
-- INNER or LEFT build with a right-side ON filter, the streaming build pops a stored block that
-- inserted no row (`HashJoin::insertFromBlockImpl`, the
-- `!flag_per_row && !is_inserted && !nullmap_stored_for_block` branch), so the block's bytes are
-- never counted. The deferred build used to buffer EVERY scattered right block and check
-- `max_bytes_in_join` over all of them, so it threw where `hash` succeeded. It now skips buffering a
-- per-slot block with no kept row (NULL key or passing the right ON mask), mirroring the pop, but only
-- when the slot's HashJoin would actually pop zero-insert blocks (`HashJoin::buildPopsZeroInsertBlocks`).
-- Related: https://github.com/ClickHouse/ClickHouse/pull/108129 (the deferred exact-size build).

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET parallel_hash_join_threshold = 0;          -- force ConcurrentHashJoin regardless of build size
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';      -- keep the big table as the build (right) side
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;              -- single-map build path; no distributed read

DROP TABLE IF EXISTS t_skip_build;
DROP TABLE IF EXISTS t_skip_probe;
CREATE TABLE t_skip_build (k UInt64, keep UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_skip_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- keep = 1 only for k < 2000 (a small contiguous run), so the right side is mostly whole blocks of
-- rows the ON filter rejects - exactly the blocks the streaming build pops. ANY LEFT keeps `r.keep = 1`
-- as a build-side ON mask (the rejected rows still reach the build).
INSERT INTO t_skip_build SELECT number, (number < 2000) AS keep, number * 3 FROM numbers(1000000);
INSERT INTO t_skip_probe SELECT number, number * 7 FROM numbers(1000000);

-- max_bytes_in_join = 16 MB sits above the empty per-slot maps' baseline (~6 MiB for 4 slots) and far
-- below the ~23 MiB the unfixed build buffered (all 1M right rows). `hash` keeps well under 1 MiB (it
-- pops the rejected blocks). Before the fix the parallel_hash query threw SET_SIZE_LIMIT_EXCEEDED; now
-- both succeed and must AGREE.
SELECT 'any_left_skip', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ANY LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'hash', join_overflow_mode = 'throw', max_bytes_in_join = 16000000;

SELECT 'any_left_skip', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ANY LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'parallel_hash', join_overflow_mode = 'throw', max_bytes_in_join = 16000000,
         log_comment = '04414_skip_discarded';

-- Positive control: the parallel_hash query used the deferred exact-size build (this profile event is
-- incremented only by that reserve; statistics collection is off), and the reserve is sized by the
-- inserted (keep = 1) keys (~2000), not by all 1M build keys - confirming the deferred two-level path
-- engaged and skipped the rejected blocks rather than falling back.
SYSTEM FLUSH LOGS query_log;
SELECT 'deferred build engaged',
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = count(),
    max(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables']) BETWEEN 1 AND 60000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment = '04414_skip_discarded';

DROP TABLE t_skip_build;
DROP TABLE t_skip_probe;
