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
);

-- Generic array keys retain their values when fingerprints and rows are coalesced together.
SELECT count(), sum(k[1]) FROM
(
    SELECT DISTINCT [number % 4093] AS k FROM numbers(131071)
);

-- Header constants remain compact while uneven strings exercise the sorting unit's byte budget.
SELECT count(), sum(length(k)), min(length(payload)), max(length(payload)) FROM
(
    SELECT DISTINCT concat(toString(number % 4093), repeat('x', if(number % 4093 % 32 = 0, 8192, 8))) AS k,
        repeat('y', 65536) AS payload
    FROM numbers(131071)
);

-- Restoring input order after coalesced deduplication retains the requested descending prefix.
SELECT count(), groupArray(k) = arrayReverseSort(groupArray(k)) FROM
(
    SELECT DISTINCT toFixedString(toString(number % 4093), 128) AS k FROM numbers(131071)
    ORDER BY concat(k, 'x') DESC LIMIT 300
);
