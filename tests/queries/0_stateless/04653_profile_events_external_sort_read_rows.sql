-- Rows re-read from the temporary files of an external sort must not be counted again
-- as source reads. Both spilling sources are covered: at max_bytes_before_external_sort =
-- 100000 only BufferingFromFileSource emits, at 1000000 MergeSorterSource emits too.
-- Every run reads exactly 100000 rows from numbers(), so read_rows and SelectedRows must
-- be 100000 on every path. current_database scopes the server-global query_log, so
-- concurrent copies of this test cannot read each other's rows.

-- in-memory control: a fix that deflates every path must still look wrong here
SELECT number FROM numbers(100000) ORDER BY sipHash64(number)
SETTINGS log_queries = 1, log_comment = '04653_memory',
    max_threads = 1, max_memory_usage = 134217728,
    max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0
FORMAT Null;

-- spills 2 parts, all re-read rows come from BufferingFromFileSource
SELECT number FROM numbers(100000) ORDER BY sipHash64(number)
SETTINGS log_queries = 1, log_comment = '04653_spill_buffering',
    max_threads = 1, max_memory_usage = 134217728,
    max_bytes_before_external_sort = 100000, max_bytes_ratio_before_external_sort = 0
FORMAT Null;

-- spills 1 part, the rest stays in memory and is re-read by MergeSorterSource
SELECT number FROM numbers(100000) ORDER BY sipHash64(number)
SETTINGS log_queries = 1, log_comment = '04653_spill_mergesorter',
    max_threads = 1, max_memory_usage = 134217728,
    max_bytes_before_external_sort = 1000000, max_bytes_ratio_before_external_sort = 0
FORMAT Null;

-- top-K never spills; must-not-regress row
SELECT number FROM numbers(100000) ORDER BY sipHash64(number) LIMIT 10
SETTINGS log_queries = 1, log_comment = '04653_limit',
    max_threads = 1, max_memory_usage = 134217728,
    max_bytes_before_external_sort = 100000, max_bytes_ratio_before_external_sort = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The spilling cells also assert that the spill really happened, otherwise they would
-- silently degenerate into extra in-memory runs and the test would be vacuous.
SELECT
    log_comment AS cell,
    if(argMax(read_rows, event_time_microseconds) = 100000
        AND argMax(ProfileEvents['SelectedRows'], event_time_microseconds) = 100000
        AND (cell IN ('04653_memory', '04653_limit')
             OR argMax(ProfileEvents['ExternalSortWritePart'], event_time_microseconds) > 0),
        'ok',
        'fail: read_rows=' || toString(argMax(read_rows, event_time_microseconds))
            || ' SelectedRows=' || toString(argMax(ProfileEvents['SelectedRows'], event_time_microseconds))
            || ' ExternalSortWritePart=' || toString(argMax(ProfileEvents['ExternalSortWritePart'], event_time_microseconds))) AS result
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment LIKE '04653\_%'
GROUP BY cell
ORDER BY cell;
