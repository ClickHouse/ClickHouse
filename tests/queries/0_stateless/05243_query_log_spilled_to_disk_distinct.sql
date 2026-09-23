-- `system.query_log.spilled_to_disk` names the operators that wrote data to temporary files, and a
-- `DISTINCT` that runs out of its memory budget is one of them. The same `DISTINCT` with spilling
-- disabled must report nothing, and a spilling `ORDER BY` must still report itself, so neither a
-- value reported unconditionally nor a column that stopped reporting can pass.

SET log_queries = 1;

-- The one-byte budget with exact memory accounting and a pinned block size makes the spill
-- deterministic. The deduplicated expression must not follow the order of the input: a `DISTINCT` on
-- a prefix of the input order is deduplicated range by range and never needs temporary files.
SELECT DISTINCT number % 100000 AS k FROM numbers(300000)
FORMAT Null
SETTINGS log_comment = '05243_spill_a_distinct_spilled', max_bytes_before_external_distinct = 1,
    max_bytes_ratio_before_external_distinct = 0, max_block_size = 65409, max_untracked_memory = 0;

-- Both thresholds at zero disable spilling, so the same query writes nothing.
SELECT DISTINCT number % 100000 AS k FROM numbers(300000)
FORMAT Null
SETTINGS log_comment = '05243_spill_b_distinct_in_memory', max_bytes_before_external_distinct = 0,
    max_bytes_ratio_before_external_distinct = 0;

SELECT a FROM (SELECT number AS a FROM numbers(100000)) ORDER BY a
FORMAT Null
SETTINGS log_comment = '05243_spill_c_sort_spilled', max_bytes_before_external_sort = 100000,
    max_bytes_ratio_before_external_sort = 0;

SYSTEM FLUSH LOGS query_log;

-- The temporary-file counters are read from the same rows, so a query that stopped spilling is told
-- apart from a column that stopped reporting. The second segment of the comment keeps the statements
-- that carry the runner's own per-test comment out of the result.
SELECT log_comment, spilled_to_disk,
       ProfileEvents['ExternalDistinctWritePart'] > 0 AS distinct_wrote_temporary_files,
       ProfileEvents['ExternalSortWritePart'] > 0 AS sort_wrote_temporary_files
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05243\_spill\_%'
ORDER BY log_comment;
