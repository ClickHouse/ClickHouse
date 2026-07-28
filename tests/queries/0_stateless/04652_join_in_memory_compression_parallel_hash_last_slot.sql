-- Tags: no-random-settings
-- (the scenario asserts byte thresholds and relies on an exact right-side block layout: randomized
-- block-size / memory settings shift both and make it meaningless)

-- Regression test for the `parallel_hash` slot-accounting hole in the "compress before spilling"
-- contract of `enable_join_in_memory_compression`.
--
-- `ConcurrentHashJoin` fills the slots of one source block sequentially and publishes each slot's
-- byte delta to the join's global counter only *after* that slot's insert returns, while the
-- compression trigger runs at the end of the insert. A threshold crossed by the *last* non-empty
-- slot of a source block was therefore observed by no slot at all (every earlier slot saw a lower
-- total), and the next source block was met first by `SpillingHashJoin`'s pre-insert check, which
-- switched to `GraceHashJoin` before anything compressed.
--
-- Here every source block of the build side carries a single key value, so it is routed to exactly
-- one slot: that slot is always the last non-empty one, and the crossing of
-- `max_bytes_before_external_join / 2` always happens inside it, while the slot's own count stays
-- far below the threshold. The trigger must add the running insert's unpublished delta back to the
-- global total, otherwise the join spills instead of compressing.

DROP TABLE IF EXISTS jimc_ls_left;

CREATE TABLE jimc_ls_left (k UInt64) ENGINE = Memory;
INSERT INTO jimc_ls_left SELECT number FROM numbers(100);

-- Control: with compression off the build side (~60 MiB) crosses half of the 24 MB threshold and
-- the join spills, which is what the compression-on runs below must avoid.
SELECT sum(cityHash64(l.k, r.rv, r.pad)) > 0
FROM jimc_ls_left AS l
INNER JOIN (SELECT intDiv(number, 600) AS k, number AS rv, repeat('x', 1000) AS pad FROM numbers(60000)) AS r
ON l.k = r.k
SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, max_block_size = 600,
         enable_join_in_memory_compression = 0, max_bytes_in_join = 0,
         max_bytes_before_external_join = 24000000, query_plan_join_swap_table = 'false',
         log_comment = '04652_off', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04652_off' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- With compression on the crossing slot compresses at the end of its own insert, the compressed
-- build side stays below the threshold, and the join never switches to `GraceHashJoin`. The result
-- must match the unlimited run.
SELECT (SELECT sum(cityHash64(l.k, r.rv, r.pad))
        FROM jimc_ls_left AS l
        INNER JOIN (SELECT intDiv(number, 600) AS k, number AS rv, repeat('x', 1000) AS pad FROM numbers(60000)) AS r
        ON l.k = r.k
        SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, max_block_size = 600,
                 enable_join_in_memory_compression = 0, max_bytes_before_external_join = 0,
                 query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, r.rv, r.pad))
        FROM jimc_ls_left AS l
        INNER JOIN (SELECT intDiv(number, 600) AS k, number AS rv, repeat('x', 1000) AS pad FROM numbers(60000)) AS r
        ON l.k = r.k
        SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, max_block_size = 600,
                 enable_join_in_memory_compression = 1, max_bytes_in_join = 0,
                 max_bytes_before_external_join = 24000000, query_plan_join_swap_table = 'false')
SETTINGS log_comment = '04652_on', enable_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'], ProfileEvents['JoinInMemoryCompressedColumns'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04652_on' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE jimc_ls_left;
