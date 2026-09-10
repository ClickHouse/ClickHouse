SET query_plan_convert_distinct_to_aggregation = 1;
SET enable_adaptive_aggregator = 0;
SET distinct_overflow_mode = 'throw';
SET max_threads = 4;
SET max_block_size = 1000;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
SET optimize_distinct_in_order = 0;
SET group_by_two_level_threshold_bytes = 0;

-- The final `DISTINCT` uses aggregation, while preliminary deduplication remains a distinct transform.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0, countIf(explain LIKE '%DistinctTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000));
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) SETTINGS query_plan_convert_distinct_to_aggregation = 0);

-- Disabling plan optimizations retains `DISTINCT`; serialization settings allow initiator-side conversion.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) SETTINGS query_plan_enable_optimizations = 0);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) SETTINGS serialize_query_plan = 1);

-- Configured `DISTINCT` limits do not prevent conversion in `throw` mode.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) SETTINGS max_rows_in_distinct = 10000, max_bytes_in_distinct = 10000000);

-- A global input order is preserved even when its leading column is outside the `DISTINCT` keys.
SET query_plan_remove_redundant_sorting = 0;
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM (SELECT number FROM numbers(100) ORDER BY number));
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 3 AS k FROM numbers(100) ORDER BY number);

-- Positional consumers in the current query or an outer query retain streaming deduplication.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) LIMIT 3);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) OFFSET 3);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) LIMIT -3);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) LIMIT 0.1);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) LIMIT 1 BY number % 3);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT DISTINCT number FROM numbers_mt(10000)) WHERE number % 2 = 0 LIMIT 3);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT DISTINCT number FROM numbers_mt(10000)) WHERE number % 2 = 0 OFFSET 3);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT DISTINCT number FROM numbers_mt(10000)) WHERE number % 2 = 0 LIMIT 1 BY number % 3);

-- An unbounded source must continue producing distinct rows as it is read.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM system.numbers);

-- Constant output columns retain their original header representation.
-- With only constant keys, `DISTINCT` retains its early stopping behavior.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT 1 FROM numbers_mt(10000));
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number, 1 FROM numbers_mt(10000));
SELECT count() FROM (SELECT DISTINCT number FROM numbers(0));
SELECT count() FROM (SELECT DISTINCT 1 FROM numbers(0));
SELECT count() FROM (SELECT DISTINCT 1 FROM numbers(10));

-- Both single-level and two-level aggregation preserve the complete set of typed keys.
SET max_bytes_in_distinct = 100000000;
SET group_by_two_level_threshold = 0;
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
SET group_by_two_level_threshold = 1;
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

-- Floating keys retain the hash equality used by unsorted `DISTINCT`, including signed zero and NaNs.
SELECT arraySort(groupArray(reinterpretAsUInt64(k)))
FROM (SELECT DISTINCT reinterpretAsFloat64([toUInt64(0), 9223372036854775808, 9221120237041090561, 9221120237041090562, 0][number + 1]) AS k
      FROM numbers_mt(5));

-- A parallel single-level merge and a two-level merge enforce the union's cardinality.
SET max_bytes_in_distinct = 0;
SET group_by_two_level_threshold = 0;
SET enable_parallel_single_level_merge = 1;
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(1000000));
SET group_by_two_level_threshold = 1;
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(1000000));

-- The replacement uses `DISTINCT` limits without inheriting aggregation overflow or spill settings.
SELECT count() FROM (SELECT DISTINCT number FROM numbers_mt(10000))
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any', max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;
SELECT count() FROM (SELECT DISTINCT number % 100 FROM numbers_mt(10000)) SETTINGS max_rows_in_distinct = 100;
SELECT count() FROM (SELECT DISTINCT number FROM numbers_mt(10000)) SETTINGS max_bytes_in_distinct = 10000000;

-- Final `DISTINCT` in a set operation can use aggregation.
SELECT arraySort(groupArray(number)) FROM (SELECT number FROM numbers(3) UNION DISTINCT SELECT number FROM numbers(5));

-- Totals from an earlier aggregation pass through `DISTINCT` unchanged.
SET query_plan_remove_redundant_distinct = 0;
SELECT DISTINCT k, c FROM (SELECT number % 3 AS k, count() AS c FROM numbers(9) GROUP BY k WITH TOTALS) ORDER BY k;

-- An unordered input carrying totals retains `DISTINCT` above the original aggregation.
SELECT countIf(explain LIKE '%AggregatingTransform%')
FROM (EXPLAIN PIPELINE SELECT DISTINCT k, c FROM (SELECT number % 3 AS k, count() AS c FROM numbers(9) GROUP BY k WITH TOTALS));

-- Bounded `break` mode retains streaming `DISTINCT` even with a finite source.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) SETTINGS max_rows_in_distinct = 5, distinct_overflow_mode = 'break');

-- Bounded `break` mode stops streaming `DISTINCT` at the configured bound.
SELECT count() FROM (SELECT DISTINCT number FROM system.numbers)
SETTINGS max_threads = 1, max_block_size = 1, max_rows_in_distinct = 5, distinct_overflow_mode = 'break';
