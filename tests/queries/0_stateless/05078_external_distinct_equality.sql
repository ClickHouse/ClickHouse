-- The first two-row chunk is deduplicated by the hash set and triggers spilling. Later chunks use sort
-- equality. Signed zeros and `NaN` payloads therefore retain both binary representatives only when both
-- reach the hash set before spilling; otherwise their first representative determines the result.
SET max_threads = 1;
SET max_block_size = 2;
SET max_bytes_before_external_distinct = 1;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_untracked_memory = 0;
SET optimize_distinct_in_order = 0;
SET allow_preliminary_distinct_abandoning = 0;

-- Each input contains the same positive fillers, 1 and 2. Literal `UInt64` representations avoid
-- floating-point arithmetic changing the signs of zeros or the payloads of NaNs.
SELECT arraySort(groupArray(reinterpretAsUInt64(k)))
FROM
(
    SELECT DISTINCT reinterpretAsFloat64([toUInt64(9223372036854775808), 0, 4607182418800017408, 4611686018427387904][number + 1]) AS k
    FROM numbers(4)
)
SETTINGS log_comment = 'external_distinct_equality/zeros_before';

SELECT arraySort(groupArray(reinterpretAsUInt64(k)))
FROM
(
    SELECT DISTINCT reinterpretAsFloat64([toUInt64(4607182418800017408), 4611686018427387904, 9223372036854775808, 0][number + 1]) AS k
    FROM numbers(4)
)
SETTINGS log_comment = 'external_distinct_equality/zeros_after';

SELECT arraySort(groupArray(reinterpretAsUInt64(k)))
FROM
(
    SELECT DISTINCT reinterpretAsFloat64([toUInt64(9223372036854775808), 4607182418800017408, 0, 4611686018427387904][number + 1]) AS k
    FROM numbers(4)
)
SETTINGS log_comment = 'external_distinct_equality/zeros_across';

SELECT arraySort(groupArray(reinterpretAsUInt64(k)))
FROM
(
    SELECT DISTINCT reinterpretAsFloat64([toUInt64(9221120237041090561), 9221120237041090560, 4607182418800017408, 4611686018427387904][number + 1]) AS k
    FROM numbers(4)
)
SETTINGS log_comment = 'external_distinct_equality/nans_before';

SELECT arraySort(groupArray(reinterpretAsUInt64(k)))
FROM
(
    SELECT DISTINCT reinterpretAsFloat64([toUInt64(4607182418800017408), 4611686018427387904, 9221120237041090561, 9221120237041090560][number + 1]) AS k
    FROM numbers(4)
)
SETTINGS log_comment = 'external_distinct_equality/nans_after';

SELECT arraySort(groupArray(reinterpretAsUInt64(k)))
FROM
(
    SELECT DISTINCT reinterpretAsFloat64([toUInt64(9221120237041090561), 4607182418800017408, 9221120237041090560, 4611686018427387904][number + 1]) AS k
    FROM numbers(4)
)
SETTINGS log_comment = 'external_distinct_equality/nans_across';

-- Every placement crosses the spill transition.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('external_distinct_equality/') + 1) AS test_case,
    min(ProfileEvents['ExternalDistinctWritePart'] > 0) AS spilled,
    min(ProfileEvents['ExternalDistinctMerge'] > 0) AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, 'external_distinct_equality/')
GROUP BY log_comment
ORDER BY test_case;
