SET max_threads = 1;
SET max_block_size = 4096;
SET max_untracked_memory = 0;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_bytes_before_external_distinct = 65536;
SET max_bytes_ratio_before_external_sort = 0;
SET allow_preliminary_distinct_abandoning = 1;
SET optimize_distinct_in_order = 0;

-- A unique prefix starts spilling. The remaining chunks repeat a small set of new keys, which must
-- remain unique when the buffered tail is merged with ordinary and suppression runs.
SELECT count(), uniqExact(k), sum(k)
FROM
(
    SELECT DISTINCT if(number < 8192, number, 8192 + number % 16) AS k
    FROM numbers(32768)
)
SETTINGS log_comment = '05141_external_distinct_final_merge/numbers';

-- Nullable and string keys use the same boundaries while preserving their column representations.
SELECT count(), uniqExact(tuple(a, b))
FROM
(
    WITH if(number < 8192, number, 8192 + number % 16) AS k
    SELECT DISTINCT if(k % 5 = 0, NULL, k) AS a, toString(k) AS b
    FROM numbers(32768)
)
SETTINGS log_comment = '05141_external_distinct_final_merge/composite';

-- Restoring arrival order keeps the first twenty keys from the descending input order.
SELECT count(), groupArray(k) = arrayReverse(range(toUInt64(8188), toUInt64(8208)))
FROM
(
    SELECT DISTINCT if(number < 8192, number, 8192 + number % 16) AS k
    FROM numbers(32768)
    ORDER BY k + 1 DESC
    LIMIT 20
)
SETTINGS log_comment = '05141_external_distinct_final_merge/ordered';

-- The final output crosses the row limit and the hint together. The row limit is checked first.
SELECT count()
FROM
(
    SELECT DISTINCT if(number < 8192, number, 8192 + number % 16) AS k
    FROM numbers(32768)
    LIMIT 8204
)
SETTINGS max_rows_in_distinct = 8199; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- `BREAK` emits the whole crossing chunk before the outer `LIMIT` trims the result.
SELECT count()
FROM
(
    SELECT DISTINCT if(number < 8192, number, 8192 + number % 16) AS k
    FROM numbers(32768)
    LIMIT 8204
)
SETTINGS max_rows_in_distinct = 8199, distinct_overflow_mode = 'break';

SYSTEM FLUSH LOGS query_log;
SELECT count(), min(ProfileEvents['ExternalDistinctWritePart']) > 0,
       min(ProfileEvents['ExternalDistinctMerge']) = 1
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND startsWith(log_comment, '05141_external_distinct_final_merge/');
