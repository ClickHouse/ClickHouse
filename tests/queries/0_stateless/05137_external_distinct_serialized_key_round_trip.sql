SET max_threads = 1;
SET max_block_size = 2;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_untracked_memory = 0;
SET optimize_distinct_in_order = 0;
SET allow_preliminary_distinct_abandoning = 0;

-- Variable-length aggregate states retain every byte, including embedded zero and non-ASCII bytes.
SELECT count(), arraySort(groupArray(finalizeAggregation(s))) = arrayMap(x -> [concat(toString(x), repeat(char(0, 120, 255), 1 + x * 1024))], range(5)) FROM (SELECT DISTINCT initializeAggregation('groupArrayState', concat(toString(number % 5), repeat(char(0, 120, 255), 1 + (number % 5) * 1024))) AS s FROM numbers(20)) SETTINGS max_bytes_before_external_distinct = 0;
SELECT count(), arraySort(groupArray(finalizeAggregation(s))) = arrayMap(x -> [concat(toString(x), repeat(char(0, 120, 255), 1 + x * 1024))], range(5)) FROM (SELECT DISTINCT initializeAggregation('groupArrayState', concat(toString(number % 5), repeat(char(0, 120, 255), 1 + (number % 5) * 1024))) AS s FROM numbers(20)) SETTINGS max_bytes_before_external_distinct = 1;

-- Each state contains three 6 MiB strings and exceeds the suppression run target. Extraction must
-- accommodate the entire state and restore all of its values.
WITH repeat(repeat(char(0, 120, 255), 32), 64 * 1024) AS payload
SELECT
    count(),
    min(finalizeAggregation(s) = arrayWithConstant(3, concat(substring(finalizeAggregation(s)[1], 1, 1), payload))),
    arraySort(groupArray(substring(finalizeAggregation(s)[1], 1, 1)))
FROM
(
    SELECT DISTINCT initializeAggregation('groupArrayArrayState', arrayWithConstant(3, concat(toString(number % 3), payload))) AS s
    FROM numbers(8)
) SETTINGS max_bytes_before_external_distinct = 0;
WITH repeat(repeat(char(0, 120, 255), 32), 64 * 1024) AS payload
SELECT
    count(),
    min(finalizeAggregation(s) = arrayWithConstant(3, concat(substring(finalizeAggregation(s)[1], 1, 1), payload))),
    arraySort(groupArray(substring(finalizeAggregation(s)[1], 1, 1)))
FROM
(
    SELECT DISTINCT initializeAggregation('groupArrayArrayState', arrayWithConstant(3, concat(toString(number % 3), payload))) AS s
    FROM numbers(8)
) SETTINGS max_bytes_before_external_distinct = 1;
