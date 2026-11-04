-- External `LIMIT BY` with a negative limit, and its reporting: `rows_before_limit_at_least`, the
-- temporary-file profile events and `system.query_log.spilled_to_disk`. The reference results match execution
-- with spilling disabled. The threshold is compared with the memory tracked by the query, so allocations must
-- not linger in the thread-local untracked buffers.
SET max_threads = 1;
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_untracked_memory = 0;
SET prefer_external_sort_block_bytes = 1048576;
SET temporary_files_buffer_size = 1048576;

-- `LIMIT -n OFFSET m BY` skips the first `m` rows of each key with a spilling `LIMIT BY` whose window has no
-- end. Its order restoration decides which rows are the last `n` of the rest, so the query returns the exact
-- rows of the in-memory one, in the sorted order.
SET max_block_size = 8192;

SELECT count(), groupArray(v) = arrayReverseSort(groupArray(v)), sum(cityHash64(k, v)) FROM
(
    SELECT k, v FROM (SELECT toString(number % 1000) AS k, number AS v FROM numbers(100000))
    ORDER BY v DESC LIMIT -2 OFFSET 3 BY k
)
SETTINGS max_bytes_before_external_limit_by = 1, log_comment = '05343_external_limit_by_limits_metrics/negative_limit';

-- With `exact_rows_before_limit`, `rows_before_limit_at_least` counts every row that the `LIMIT BY` returns.
-- Each key has two adjacent rows, and `OFFSET 1` keeps the second one. At 96 MiB the string keys fill the
-- hash table for three or four chunks of 65536 rows before the spill, so the query returns rows during
-- hashing and from the spill merge, and the statistic must include both portions of the result. The sort
-- below the `LIMIT BY` must not spill itself, so that the memory it holds is the same in every run.
SET max_block_size = 65536;
SET max_bytes_before_external_sort = 0;
SET max_bytes_ratio_before_external_sort = 0;
SET output_format_write_statistics = 0;
SET exact_rows_before_limit = 1;

SELECT v FROM
(
    SELECT concat(toString(intDiv(number, 2)), repeat('x', 100)) AS k, toUInt32(number) AS v FROM numbers(600000)
)
ORDER BY v DESC LIMIT 1 OFFSET 1 BY k LIMIT 3
FORMAT JSONCompact SETTINGS max_bytes_before_external_limit_by = 100663296,
    log_comment = '05343_external_limit_by_limits_metrics/rows_before_limit_after_hashing';

SET exact_rows_before_limit = DEFAULT;

-- `system.query_log.spilled_to_disk` names the operators that wrote data to temporary files, and the
-- temporary-file profile events count those files and their bytes. The same `LIMIT BY` with spilling disabled
-- reports nothing, so a value reported unconditionally cannot pass.
SELECT k, v FROM (SELECT number % 100000 AS k, number AS v FROM numbers(300000)) LIMIT 1 OFFSET 1 BY k
FORMAT Null
SETTINGS max_bytes_before_external_limit_by = 1, log_comment = '05343_external_limit_by_limits_metrics/spilled';
SELECT k, v FROM (SELECT number % 100000 AS k, number AS v FROM numbers(300000)) LIMIT 1 OFFSET 1 BY k
FORMAT Null
SETTINGS max_bytes_before_external_limit_by = 0, log_comment = '05343_external_limit_by_limits_metrics/in_memory';

SET max_block_size = DEFAULT;
SET max_untracked_memory = DEFAULT;

SYSTEM FLUSH LOGS query_log;

-- Every spilling query wrote runs and merged them once.
SELECT
    substring(log_comment, length('05343_external_limit_by_limits_metrics/') + 1) AS test_case,
    ProfileEvents['ExternalLimitByWritePart'] > 0 AS spilled,
    ProfileEvents['ExternalLimitByMerge'] AS merges
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05343_external_limit_by_limits_metrics/')
ORDER BY test_case;

-- The temporary files and their bytes are counted, and `spilled_to_disk` names `limit_by` only for the
-- spilling query.
SELECT
    substring(log_comment, length('05343_external_limit_by_limits_metrics/') + 1) AS test_case,
    spilled_to_disk,
    ProfileEvents['ExternalLimitByCompressedBytes'] > 0 AS compressed_bytes,
    ProfileEvents['ExternalLimitByUncompressedBytes'] > 0 AS uncompressed_bytes,
    ProfileEvents['ExternalProcessingFilesTotal'] > 0 AS files
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN
        ('05343_external_limit_by_limits_metrics/spilled', '05343_external_limit_by_limits_metrics/in_memory')
ORDER BY test_case;
