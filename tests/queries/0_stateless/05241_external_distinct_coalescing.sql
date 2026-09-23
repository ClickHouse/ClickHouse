SET max_threads = 1;
SET max_block_size = 128;
SET max_memory_usage = 134217728;
SET max_bytes_before_external_distinct = 33554432;
SET max_bytes_ratio_before_external_distinct = 0;
SET prefer_external_sort_block_bytes = 65536;
SET optimize_distinct_in_order = 0;
SET max_untracked_memory = 0;

-- Duplicates span input chunks and sorting units, including an incomplete final unit.
SELECT count(), sum(k) FROM
(
    SELECT DISTINCT number % 4093 AS k FROM numbers(131071)
)
SETTINGS log_comment = '05241_external_distinct_coalescing/numeric';

-- Generic array keys retain their values when fingerprints and rows are coalesced together.
SELECT count(), sum(k[1]) FROM
(
    SELECT DISTINCT [number % 4093] AS k FROM numbers(131071)
)
SETTINGS log_comment = '05241_external_distinct_coalescing/array';

-- Header constants remain compact while uneven strings exercise the sorting unit's byte budget.
SELECT count(), sum(length(k)), min(length(payload)), max(length(payload)) FROM
(
    SELECT DISTINCT concat(toString(number % 4093), repeat('x', if(number % 4093 % 32 = 0, 8192, 8))) AS k,
        repeat('y', 65536) AS payload
    FROM numbers(131071)
)
SETTINGS log_comment = '05241_external_distinct_coalescing/uneven';

-- Restoring input order after coalesced deduplication retains the requested descending prefix.
SELECT count(), groupArray(k) = arrayReverseSort(groupArray(k)) FROM
(
    SELECT DISTINCT toFixedString(toString(number % 4093), 128) AS k FROM numbers(131071)
    ORDER BY concat(k, 'x') DESC LIMIT 300
)
SETTINGS log_comment = '05241_external_distinct_coalescing/ordered';

-- Every case must write a temporary run and merge it back into the distinct result.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05241_external_distinct_coalescing/') + 1) AS test_case,
    min(ProfileEvents['ExternalDistinctWritePart'] > 0) AS spilled,
    min(ProfileEvents['ExternalDistinctMerge'] > 0) AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05241_external_distinct_coalescing/')
GROUP BY log_comment
ORDER BY test_case;
