SET max_threads = 1;
SET max_block_size = 1024;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_bytes_before_external_distinct = 1;
SET prefer_external_sort_block_bytes = 65536;
SET max_untracked_memory = 0;
SET optimize_distinct_in_order = 0;

-- Duplicates span spill files and their smaller blocks, while wide values survive readback intact.
SELECT count(), sum(length(k)) FROM
(
    SELECT DISTINCT toFixedString(toString(number % 2048), 1024) AS k FROM numbers(16384)
)
SETTINGS log_comment = '05240_external_distinct_spill_blocks/wide';

-- Uneven row widths can exceed the average byte target without losing keys at block boundaries.
SELECT count(), sum(length(k)) FROM
(
    SELECT DISTINCT concat(toString(number % 2048), repeat('x', if(number % 8 = 0, 8192, 8))) AS k
    FROM numbers(16384)
)
SETTINGS log_comment = '05240_external_distinct_spill_blocks/uneven';

-- The sort restoring input order can also spill, and its smaller blocks preserve descending order.
SELECT count(), groupArray(k) = arrayReverseSort(groupArray(k)) FROM
(
    SELECT DISTINCT toFixedString(toString(number % 2048), 1024) AS k
    FROM numbers(16384)
    ORDER BY concat(k, 'x') DESC
    LIMIT 300
)
SETTINGS log_comment = '05240_external_distinct_spill_blocks/ordered';

-- Disabling the byte target preserves the same distinct values with blocks sized by rows.
SELECT count(), sum(length(k)) FROM
(
    SELECT DISTINCT toFixedString(toString(number % 2048), 1024) AS k FROM numbers(16384)
)
SETTINGS prefer_external_sort_block_bytes = 0, log_comment = '05240_external_distinct_spill_blocks/row_sized';

-- Every case must write a temporary run and merge it back into the distinct result.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05240_external_distinct_spill_blocks/') + 1) AS test_case,
    min(ProfileEvents['ExternalDistinctWritePart'] > 0) AS spilled,
    min(ProfileEvents['ExternalDistinctMerge'] > 0) AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05240_external_distinct_spill_blocks/')
GROUP BY log_comment
ORDER BY test_case;
