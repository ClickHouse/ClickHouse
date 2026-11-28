SET max_threads = 1;
SET max_block_size = 4096;
SET max_untracked_memory = 0;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_bytes_before_external_distinct = 131072;
SET max_bytes_ratio_before_external_sort = 0;
SET allow_preliminary_distinct_abandoning = 1;
SET optimize_distinct_in_order = 0;

-- The unique prefix starts spilling before the repeated keys arrive. Several locally unique chunks
-- accumulate in each ordinary run, and their overlapping keys must be merged into one occurrence.
SELECT count(), uniqExact(k), sum(k)
FROM
(
    SELECT DISTINCT if(number < 8192, number + 1000000, number % 4096) AS k
    FROM numbers(524288)
)
SETTINGS log_comment = '05140_external_distinct_merge_unique_chunks/numbers';

-- Composite keys include nullable values and strings. Equal nullable prefixes are distinguished by
-- the second key, including when the same pair occurs in different chunks of a run.
SELECT count(), sum(a)
FROM
(
    WITH if(number < 8192, number + 1000000, number % 4096) AS k
    SELECT DISTINCT if(k % 17 = 0, NULL, k) AS a, toString(k % 7) AS b
    FROM numbers(524288)
)
SETTINGS log_comment = '05140_external_distinct_merge_unique_chunks/composite';

-- Ordering by an expression carries a non-key column and arrival numbers through the spilled runs.
SELECT count(), uniqExact(k), groupArray(k) = arrayReverseSort(groupArray(k))
FROM
(
    SELECT DISTINCT if(number < 8192, number + 1000000, number % 4096) AS k
    FROM numbers(524288)
    ORDER BY k + 1 DESC
)
SETTINGS log_comment = '05140_external_distinct_merge_unique_chunks/ordered';

SYSTEM FLUSH LOGS query_log;
SELECT count(), min(ProfileEvents['ExternalDistinctWritePart']) > 2,
       min(ProfileEvents['ExternalDistinctMerge']) = 1
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND startsWith(log_comment, '05140_external_distinct_merge_unique_chunks/');
