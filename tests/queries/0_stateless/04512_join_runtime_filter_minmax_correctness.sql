SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 1;
SET join_runtime_filter_from_fixed_hash_table = 0;
SET join_runtime_filter_size_from_hash_table_stats = 0;
SET join_runtime_filter_use_minmax = 1;

SELECT 'numeric_minmax_selective', count()
FROM (SELECT number AS k FROM numbers(1000)) AS p
INNER JOIN (SELECT number + 10 AS k FROM numbers(10)) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128;

SELECT 'numeric_minmax_full_range', count()
FROM (SELECT number AS k FROM numbers(100)) AS p
INNER JOIN (SELECT number AS k FROM numbers(100)) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128;

SELECT 'empty_build', count()
FROM (SELECT number AS k FROM numbers(10)) AS p
INNER JOIN (SELECT number AS k FROM numbers(0)) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128;

SELECT 'nullable_exact_fallback', count()
FROM (SELECT if(number = 0, NULL, toNullable(number)) AS k FROM numbers(5)) AS p
INNER JOIN (SELECT if(number = 0, NULL, toNullable(number)) AS k FROM numbers(3)) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 10000;

SELECT 'signed_unsigned_common_type', count()
FROM (SELECT toInt64(number) - 2 AS k FROM numbers(5)) AS p
INNER JOIN (SELECT toUInt8(number) AS k FROM numbers(3)) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128;

SELECT 'float_nan_exact', count()
FROM (SELECT toFloat64('nan') AS k) AS p
INNER JOIN (SELECT toFloat64('nan') AS k) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 10000;

SELECT 'float_nan_minmax', count()
FROM (SELECT toFloat64('nan') AS k) AS p
INNER JOIN (SELECT toFloat64('nan') AS k) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128;

SELECT 'left_anti_nan_exact', count()
FROM (SELECT toFloat64('nan') AS k) AS p
LEFT ANTI JOIN (SELECT toFloat64('nan') AS k) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 10000;

SELECT 'date_key', count()
FROM (SELECT toDate('2020-01-01') + number AS k FROM numbers(5)) AS p
INNER JOIN (SELECT toDate('2020-01-03') AS k) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128;

SELECT 'datetime_key', count()
FROM (SELECT toDateTime('2020-01-01 00:00:00') + number AS k FROM numbers(5)) AS p
INNER JOIN (SELECT toDateTime('2020-01-01 00:00:03') AS k) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128;

SELECT 'decimal_key', count()
FROM (SELECT toDecimal64(number, 2) AS k FROM numbers(5)) AS p
INNER JOIN (SELECT toDecimal64(3, 2) AS k) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128;

SELECT 'string_key', count()
FROM (SELECT toString(number) AS k FROM numbers(5)) AS p
INNER JOIN (SELECT '3' AS k) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 10000;

SELECT 'fixed_string_key', count()
FROM (SELECT toFixedString(toString(number), 4) AS k FROM numbers(5)) AS p
INNER JOIN (SELECT toFixedString('3', 4) AS k) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128;

SELECT 'parallel_merge_minmax', count()
FROM (SELECT number AS k FROM numbers_mt(10000)) AS p
INNER JOIN (SELECT number + 100 AS k FROM numbers_mt(100)) AS b ON p.k = b.k
SETTINGS join_runtime_filter_exact_values_limit = 0, join_runtime_bloom_filter_bytes = 128, max_threads = 4;
