SET query_plan_convert_distinct_to_aggregation = 1;
SET enable_adaptive_aggregator = 1;
SET enable_packed_string_keys_in_aggregation = 1;
SET serialize_string_in_memory_with_zero_byte = 1;
SET collect_hash_table_stats_during_aggregation = 0;
SET distinct_overflow_mode = 'throw';
SET max_threads = 4;
SET max_block_size = 1000;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 100000000;
SET optimize_distinct_in_order = 0;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 0;
SET adaptive_aggregator_freeze_threshold = 0;
SET adaptive_aggregator_freeze_threshold_bytes = 0;

-- Immediate freezing stages every key, including records coalesced from small input blocks.
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(100000));
SELECT count(), sum(length(k))
FROM (SELECT DISTINCT concat(repeat('x', 60), toString(number)) AS k FROM numbers_mt(100000));
SELECT count(), sum(a), sum(b)
FROM (SELECT DISTINCT number % 1000 AS a, number % 13 AS b FROM numbers_mt(100000));
SELECT count() FROM (SELECT DISTINCT [number, number + 1] FROM numbers_mt(100000));

-- Both string hash-table representations and serialized keys retain their bytes through merging.
SELECT count() FROM (SELECT DISTINCT toString(number) FROM numbers_mt(100000))
SETTINGS enable_packed_string_keys_in_aggregation = 0;
SELECT count() FROM (SELECT DISTINCT tuple(number, toString(number)) FROM numbers_mt(100000))
SETTINGS serialize_string_in_memory_with_zero_byte = 0;

-- Adaptive-enabled aggregation preserves fixed-width, serialized, and nullable key representations.
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toUInt8(number % 3) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toUInt16(number % 3) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toUInt32(number % 3) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT number % 3 FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toUInt128(number % 3) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toUInt256(number % 3) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toFloat64(number % 3) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toString(number % 3) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toString(number % 3) FROM numbers_mt(10000))
SETTINGS enable_packed_string_keys_in_aggregation = 0;
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toFixedString(toString(number % 3), 8) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT CAST(toString(number % 3), 'LowCardinality(String)') FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT if(number % 3 = 0, NULL, toUInt32(number % 3)) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT CAST(if(number % 3 = 0, NULL, toString(number % 3)), 'LowCardinality(Nullable(String))') FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toUInt16(number % 3), toUInt16(number % 2) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toUInt32(number % 3), toUInt32(number % 2) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT number % 3, number % 2 FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toUInt128(number % 3), toUInt128(number % 2) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT if(number % 3 = 0, NULL, number % 3), number % 2 FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toString(number % 3), toString(number % 2) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT toString(number % 3), toString(number % 2) FROM numbers_mt(10000))
SETTINGS serialize_string_in_memory_with_zero_byte = 0;
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT if(number % 3 = 0, NULL, toString(number % 3)), toString(number % 2) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT [number % 3, number % 2] FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT map('k', number % 3) FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM (SELECT DISTINCT tuple(number % 3, toString(number % 2)) FROM numbers_mt(10000));
-- Staged floating-point keys retain the binary identity of signed zero and NaN payloads.
SELECT arraySort(groupArray(reinterpretAsUInt64(k))) FROM
(
    SELECT DISTINCT reinterpretAsFloat64(arrayElement(
        [toUInt64(0), 9223372036854775808, 9221120237041090561, 9221120237041090562], number % 4 + 1)) AS k
    FROM numbers_mt(10000)
);

-- Large blocks publish staged chunks without coalescing, while wider keys also split at publication.
SELECT count() FROM (SELECT DISTINCT number FROM numbers_mt(1000000)) SETTINGS max_block_size = 1000000;
SELECT count() FROM (SELECT DISTINCT concat(repeat('x', 1000), toString(number)) FROM numbers_mt(100000))
SETTINGS max_block_size = 100000, max_bytes_in_distinct = 1000000000;

-- Freezing by key count and by bytes preserves the same key set.
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(100000))
SETTINGS adaptive_aggregator_freeze_threshold = 100;
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(100000))
SETTINGS adaptive_aggregator_freeze_threshold = 1000000, adaptive_aggregator_freeze_threshold_bytes = 1;
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(100000))
SETTINGS max_bytes_in_distinct = 0;
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(100000))
SETTINGS enable_adaptive_aggregator = 0;

-- The global row bound includes staged keys even when every local hash table stays empty.
SELECT number AS k FROM numbers(100) UNION DISTINCT SELECT number + 100 AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT number AS k FROM numbers(100) UNION DISTINCT SELECT number AS k FROM numbers(100))
SETTINGS max_rows_in_distinct = 100;

-- Pending and published staging buffers participate in the byte bound before the final merge.
SELECT number AS k FROM numbers(100000) UNION DISTINCT SELECT number + 100000 AS k FROM numbers(100000)
SETTINGS max_bytes_in_distinct = 1000000 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT concat(repeat('x', 60), toString(number)) AS k FROM numbers(10000)
UNION DISTINCT SELECT concat(repeat('y', 60), toString(number)) AS k FROM numbers(10000)
SETTINGS max_bytes_in_distinct = 1000000 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Repeated staged keys can thaw the local tables without changing the final set.
SELECT count(), sum(k) FROM
(
    SELECT number % 1000 AS k FROM numbers(2000000)
    UNION DISTINCT SELECT number % 1000 AS k FROM numbers(2000000)
);

-- Ordinary aggregation stages counts and aggregate arguments independently of `DISTINCT` limits.
SELECT count(), sum(c) FROM (SELECT number, count() AS c FROM numbers_mt(100000) GROUP BY number)
SETTINGS max_rows_in_distinct = 1, max_bytes_in_distinct = 1;
SELECT count(), sum(s) FROM (SELECT number, sum(number) AS s FROM numbers_mt(100000) GROUP BY number)
SETTINGS max_rows_in_distinct = 1, max_bytes_in_distinct = 1;

-- Ordinary aggregation keeps its row-limit exception and dropping modes.
SELECT number FROM numbers(100) GROUP BY number
SETTINGS max_rows_to_group_by = 10, group_by_overflow_mode = 'throw' FORMAT Null; -- { serverError TOO_MANY_ROWS }
SELECT count() > 0 FROM (SELECT number FROM numbers(100) GROUP BY number)
SETTINGS max_rows_to_group_by = 10, group_by_overflow_mode = 'break';
SELECT count() FROM (SELECT number FROM numbers(100) GROUP BY number)
SETTINGS max_rows_to_group_by = 10, group_by_overflow_mode = 'any', max_block_size = 1, max_threads = 1;
