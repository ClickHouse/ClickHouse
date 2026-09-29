SET max_threads = 1;
SET max_block_size = 2;
SET max_bytes_before_external_distinct = 1;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_untracked_memory = 0;
SET optimize_distinct_in_order = 0;
SET allow_preliminary_distinct_abandoning = 0;

SELECT count(), uniqExact(reinterpretAsUInt64(f), s)
FROM
(
    SELECT DISTINCT
        reinterpretAsFloat64([toUInt64(9223372036854775808), 0, 4609434218613702656][number + 1]) AS f,
        ['same', 'same', 'other'][number + 1] AS s
    FROM numbers(3)
);
