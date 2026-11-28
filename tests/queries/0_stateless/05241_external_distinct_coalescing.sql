SET max_threads = 1;
SET max_block_size = 1024;
SET max_bytes_before_external_distinct = 33554432;
SET max_bytes_ratio_before_external_distinct = 0;
SET prefer_external_sort_block_bytes = 65536;
SET optimize_distinct_in_order = 0;
SET max_untracked_memory = 0;

-- The spill threshold switches `DISTINCT` to external mode only once its state exceeds the smaller of the
-- threshold and about 16 MiB. Each case first retains enough distinct keys to pass that floor; the rows after
-- the switch then go through the sorting units.

-- Duplicates span input chunks and sorting units, including an incomplete final unit.
SELECT count(), sum(k) FROM
(
    SELECT DISTINCT toUInt64(if(number < 600000, number, 1000000 + number % 4093)) AS k FROM numbers(731071)
)
SETTINGS log_comment = '05241_external_distinct_coalescing/numeric', max_memory_usage = 134217728;

-- Generic array keys retain their values when fingerprints and rows are coalesced together.
SELECT count(), sum(k[1]) FROM
(
    SELECT DISTINCT [toUInt64(if(number < 200000, number, 1000000 + number % 4093))] AS k FROM numbers(331071)
)
SETTINGS log_comment = '05241_external_distinct_coalescing/array', max_memory_usage = 134217728;

-- Header constants remain compact while uneven strings exercise the sorting unit's byte budget.
SELECT count(), sum(length(k)), min(length(payload)), max(length(payload)) FROM
(
    WITH toUInt64(if(number < 50000, number, 1000000 + number % 4093)) AS key
    SELECT DISTINCT concat(toString(key), repeat('x', if(key % 32 = 0, 8192, 8))) AS k,
        repeat('y', 65536) AS payload
    FROM numbers(181071)
)
SETTINGS log_comment = '05241_external_distinct_coalescing/uneven', max_memory_usage = 134217728;

-- Restoring input order after coalesced deduplication retains the requested descending prefix. The hinted
-- operator holds at most the keys of its limit, so the limit exceeds the keys that fill its state up to the
-- floor.
SELECT count(), groupArray(k) = arrayReverseSort(groupArray(k)) FROM
(
    SELECT DISTINCT toFixedString(toString(number % 100003), 128) AS k FROM numbers(200006)
    ORDER BY concat(k, 'x') DESC LIMIT 80000
)
SETTINGS log_comment = '05241_external_distinct_coalescing/ordered', max_memory_usage = 536870912;

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
