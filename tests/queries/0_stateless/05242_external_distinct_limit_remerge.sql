SET max_threads = 1;
SET max_block_size = 1024;
SET max_memory_usage = 536870912;
SET max_bytes_before_external_distinct = 33554432;
SET max_bytes_ratio_before_external_distinct = 0;
SET prefer_external_sort_block_bytes = 1048576;
SET optimize_distinct_in_order = 0;
SET max_untracked_memory = 0;

-- Wide surviving rows accumulate enough memory to remerge while retaining a small ordered prefix.
SELECT count(), min(toUInt64(k)), max(toUInt64(k)),
    groupArray(toUInt64(k)) = arrayReverseSort(groupArray(toUInt64(k)))
FROM
(
    SELECT DISTINCT toFixedString(toString(number), 1024) AS k FROM numbers(65536)
    ORDER BY toUInt64(k) DESC LIMIT 1000
)
SETTINGS log_comment = '05242_external_distinct_limit_remerge/small_limit';

-- The hint includes the offset and applies after duplicates across spill runs have been removed.
SELECT count(), min(toUInt64(k)), max(toUInt64(k)),
    groupArray(toUInt64(k)) = arrayReverseSort(groupArray(toUInt64(k)))
FROM
(
    SELECT DISTINCT toFixedString(toString(number % 65536), 512) AS k FROM numbers(131072)
    ORDER BY toUInt64(k) DESC LIMIT 4096 OFFSET 1024
)
SETTINGS log_comment = '05242_external_distinct_limit_remerge/offset';

-- A limit covering most surviving rows leaves too little reduction for an intermediate remerge.
SELECT count(), min(toUInt64(k)), max(toUInt64(k)),
    groupArray(toUInt64(k)) = arrayReverseSort(groupArray(toUInt64(k)))
FROM
(
    SELECT DISTINCT toFixedString(toString(number), 512) AS k FROM numbers(65536)
    ORDER BY toUInt64(k) DESC LIMIT 60000
)
SETTINGS log_comment = '05242_external_distinct_limit_remerge/large_limit';

-- Without a limit hint, order restoration keeps every distinct row.
SELECT count(), min(toUInt64(k)), max(toUInt64(k)),
    groupArray(toUInt64(k)) = arrayReverseSort(groupArray(toUInt64(k)))
FROM
(
    SELECT DISTINCT toFixedString(toString(number), 512) AS k FROM numbers(65536)
    ORDER BY toUInt64(k) DESC
)
SETTINGS log_comment = '05242_external_distinct_limit_remerge/unbounded';

-- Every case must write a temporary run and merge it back into the distinct result.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05242_external_distinct_limit_remerge/') + 1) AS test_case,
    min(ProfileEvents['ExternalDistinctWritePart'] > 0) AS spilled,
    min(ProfileEvents['ExternalDistinctMerge'] > 0) AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05242_external_distinct_limit_remerge/')
GROUP BY log_comment
ORDER BY test_case;
