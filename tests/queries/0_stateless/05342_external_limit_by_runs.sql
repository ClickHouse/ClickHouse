-- Windows of `LIMIT n OFFSET m BY` other than the `[0, 1)` of `LIMIT 1 BY` rank rows one by one in every
-- merge path of external `LIMIT BY`: sorting units and runs keep the first `m + n` rows of each key, and the
-- final merge continues the rank of each key across the blocks of a run, across runs and into the in-memory
-- tail. The reference results match execution with spilling disabled. Without `ORDER BY` the input order has
-- no meaning, so a spilling query may keep any rows of a key that fit its window: the checks cover the number
-- of rows, the number of rows of each key through a checksum of the keys, and the absence of repeated rows.
-- With `ORDER BY` the spilling query returns the exact rows of the in-memory one, in the sorted order, which
-- `groupArray` observes because the `LIMIT BY` output is a single stream. The threshold is compared with the
-- memory tracked by the query, so allocations must not linger in the thread-local untracked buffers.
SET max_threads = 1;
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_untracked_memory = 0;

-- A one-byte threshold spills at the first chunk, and every chunk becomes its own run. The byte target of
-- 64 KiB writes spill blocks of 128 rows of 1 KiB, the minimum block size, so the three or four rows of a key
-- in each run straddle block boundaries, and each key spans all sixteen runs.
SET max_block_size = 1024;
SET prefer_external_sort_block_bytes = 65536;
SET max_bytes_before_external_limit_by = 1;

-- The window `[3, 8)` continues across the blocks of each run and across runs, and wide values survive
-- readback intact.
SELECT count(), sum(length(k[1])), sum(cityHash64(k)), uniqExact(v) = count() FROM
(
    SELECT [toFixedString(toString(number % 300), 1024)] AS k, number AS v FROM numbers(16384)
    LIMIT 5 OFFSET 3 BY k
)
SETTINGS log_comment = '05342_external_limit_by_runs/blocks_wide';

-- Before the final merge, the rows collected after the last run stay in memory when they fit beside the file
-- readers. The decision follows estimates of the reader buffers, so `temporary_files_buffer_size` is pinned.
-- The 32 MiB threshold spills only a state that grows past the spill floor of about 16 MiB, so the query
-- counts a million keys, and the sort below stays in memory.
SET max_block_size = 4096;
SET prefer_external_sort_block_bytes = 1048576;
SET temporary_files_buffer_size = 1048576;

-- Order restoration merges the files with the retained in-memory tail.
SELECT count(), groupArray(v) = arrayReverseSort(groupArray(v)), sum(cityHash64(k, v)) FROM
(
    SELECT k, v FROM (SELECT cityHash64(number) % 1000000 AS k, number AS v FROM numbers(3000000))
    ORDER BY v DESC LIMIT 3 OFFSET 2 BY k
)
SETTINGS max_bytes_before_external_limit_by = 33554432, max_bytes_before_external_sort = 0,
    max_bytes_ratio_before_external_sort = 0, log_comment = '05342_external_limit_by_runs/tail_kept_ordered';

SET max_bytes_before_external_limit_by = 131072;

-- The cut keeps the earliest rows of each hot key in the sorted order, and the ranks of cold keys continue
-- across runs in arrival order, so the spilling query keeps the exact rows of the in-memory one.
SELECT count(), groupArray(v) = arrayReverseSort(groupArray(v)), sum(cityHash64(k, v)) FROM
(
    SELECT k, v FROM
    (
        SELECT if(number % 2 = 0, number % 512, 1001 + number % 4096) AS k, number AS v FROM numbers(131072)
    )
    ORDER BY v DESC LIMIT 3 OFFSET 2 BY k
)
SETTINGS log_comment = '05342_external_limit_by_runs/runs_ordered';

-- `system.query_log` also holds the queries of every other test running in parallel, so read it with the
-- default settings.
SET max_block_size = DEFAULT;
SET max_untracked_memory = DEFAULT;

-- Every spilling query wrote runs and merged them once.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05342_external_limit_by_runs/') + 1) AS test_case,
    ProfileEvents['ExternalLimitByWritePart'] > 0 AS spilled,
    ProfileEvents['ExternalLimitByMerge'] AS merges
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05342_external_limit_by_runs/')
ORDER BY test_case;

-- The query with multi-chunk runs wrote more than two runs, and the query with the in-memory tail kept all of
-- it.
SELECT
    anyIf(ProfileEvents['ExternalLimitByWritePart'] > 2, log_comment = '05342_external_limit_by_runs/runs_ordered')
        AS several_runs,
    anyIf(ProfileEvents['ExternalLimitByTailSpilledRows'] = 0 AND ProfileEvents['ExternalLimitByTailKeptRows'] > 0,
        log_comment = '05342_external_limit_by_runs/tail_kept_ordered') AS tail_kept
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05342_external_limit_by_runs/');
