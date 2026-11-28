SET query_plan_convert_distinct_to_aggregation = 1;
SET distinct_overflow_mode = 'throw';
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
SET max_threads = 4;
SET max_block_size = 1000;
SET optimize_distinct_in_order = 0;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 0;

-- Mixed constant and nonconstant columns use aggregation with either aggregator implementation.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number, 1 FROM numbers_mt(10000)
      SETTINGS enable_adaptive_aggregator = 0);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number, 1 FROM numbers_mt(10000)
      SETTINGS enable_adaptive_aggregator = 1);

-- Constants keep their values, types, and positions, including repeated output names.
SET enable_adaptive_aggregator = 0;
SELECT arraySort(groupArray(tuple(*))) FROM
    (SELECT DISTINCT 'prefix', toUInt64(number % 3), NULL::Nullable(UInt64), toUInt64(number % 3), 'prefix'
     FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM
    (SELECT DISTINCT toUInt64(number % 2), [1, 2], 'suffix' FROM numbers_mt(10000));
SELECT count() FROM (SELECT DISTINCT number, 1 FROM numbers(0));
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 0;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SELECT arraySort(groupArray(tuple(*))) FROM
    (SELECT DISTINCT 'prefix', toUInt64(number % 3), NULL::Nullable(UInt64), toUInt64(number % 3), 'prefix'
     FROM numbers_mt(10000));
SELECT arraySort(groupArray(tuple(*))) FROM
    (SELECT DISTINCT toUInt64(number % 2), [1, 2], 'suffix' FROM numbers_mt(10000));
SELECT count() FROM (SELECT DISTINCT number, 1 FROM numbers(0));

-- Unique keys exercise adaptive staging while the constant column is restored after merging.
SELECT count(), sum(k), any(c)
FROM (SELECT DISTINCT number AS k, 'constant' AS c FROM numbers_mt(10000));

-- With only constant keys, `DISTINCT` stops after its first nonempty chunk.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT 1, 'constant' FROM numbers_mt(10000));
SELECT count() FROM (SELECT DISTINCT 1, 'constant' FROM numbers(0));
SELECT count() FROM (SELECT DISTINCT 1, 'constant' FROM numbers(10));
