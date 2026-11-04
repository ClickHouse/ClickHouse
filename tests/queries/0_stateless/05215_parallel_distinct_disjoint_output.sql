SET max_threads = 4;
SET max_block_size = 1000;
SET enable_parallel_replicas = 0;
SET allow_parallel_distinct = 1;
SET allow_distinct_partitions_independently = 0;
SET allow_aggregate_partitions_independently = 1;
SET allow_limit_by_partitions_independently = 1;
SET allow_window_partitions_independently = 1;
SET query_plan_enable_multithreading_after_window_functions = 0;
SET max_rows_to_group_by = 0;
SET max_rows_to_sort = 0;
SET max_bytes_to_sort = 0;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;

-- The hash-partitioned streams support independent aggregation after row expansion.
SELECT count(), sum(c), min(c), max(c) FROM (SELECT k, count() AS c FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x GROUP BY k);
SELECT countIf(explain LIKE '%Skip merging: 1%') > 0 FROM (EXPLAIN actions = 1 SELECT k, count() AS c FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x GROUP BY k);

-- A downstream `LIMIT BY` can keep the same partition assignments.
SELECT count(), uniqExact(k) FROM (SELECT k, x FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x LIMIT 2 BY k);
SELECT countIf(explain LIKE '%Skip stream merging: 1%') > 0 FROM (EXPLAIN actions = 1 SELECT k, x FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x LIMIT 2 BY k);

-- Window sorting reuses the scatter when its partition keys determine the `DISTINCT` keys.
SELECT count(), sum(s) FROM (SELECT k, sum(x) OVER (PARTITION BY k) AS s FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x);
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 1 FROM (EXPLAIN PIPELINE SELECT k, sum(x) OVER (PARTITION BY k) AS s FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x);

-- Aliases and filters preserve the partitioning expression.
SELECT count(), sum(c) FROM (SELECT renamed, count() AS c FROM (SELECT k AS renamed FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) WHERE k < 500) ARRAY JOIN [0, 1, 2] AS x GROUP BY renamed);

-- A subset of the scatter keys does not determine the stream containing a group.
SELECT count(), sum(c), min(c), max(c) FROM (SELECT a, count() AS c FROM (SELECT DISTINCT number % 20 AS a, intDiv(number, 20) % 5 AS b FROM numbers_mt(50000)) GROUP BY a);
SELECT countIf(explain LIKE '%Skip merging: 1%') = 0 FROM (EXPLAIN actions = 1 SELECT a, count() AS c FROM (SELECT DISTINCT number % 20 AS a, intDiv(number, 20) % 5 AS b FROM numbers_mt(50000)) GROUP BY a);
SELECT count(), min(c), max(c) FROM (SELECT a, count() OVER (PARTITION BY a) AS c FROM (SELECT DISTINCT number % 20 AS a, intDiv(number, 20) % 5 AS b FROM numbers_mt(50000)));
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 2 FROM (EXPLAIN PIPELINE SELECT a, count() OVER (PARTITION BY a) AS c FROM (SELECT DISTINCT number % 20 AS a, intDiv(number, 20) % 5 AS b FROM numbers_mt(50000)));

-- Non-injective expressions can group keys from different streams.
SELECT count(), sum(c), min(c), max(c) FROM (SELECT k % 10 AS g, count() AS c FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x GROUP BY g);

-- Global `BREAK` limits preserve partition assignments while results are complete.
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x GROUP BY k) SETTINGS max_rows_in_distinct = 2000, distinct_overflow_mode = 'break';
SELECT countIf(explain LIKE '%DistinctLimitsCheckingTransform%') = 1 FROM (EXPLAIN PIPELINE SELECT k, count() AS c FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x GROUP BY k) SETTINGS max_rows_in_distinct = 2000, distinct_overflow_mode = 'break';

-- A single input stream satisfies the same key-disjointness guarantee without scattering.
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x GROUP BY k) SETTINGS max_threads = 1;

-- Constant keys and empty inputs do not require hash partitioning.
SELECT count() FROM (SELECT DISTINCT 1 AS k FROM numbers_mt(50000));
SELECT count() FROM (SELECT DISTINCT number AS k FROM numbers_mt(0));

-- Consumers of globally sorted `DISTINCT` input retain its order.
SELECT groupArray(k) = range(1000) FROM (SELECT DISTINCT k FROM (SELECT number % 1000 AS k FROM numbers_mt(50000) ORDER BY k)) SETTINGS optimize_distinct_in_order = 0;


-- A second `DISTINCT` retains the stronger partitioning of its input for a downstream window.
SELECT count(), sum(c) FROM (SELECT k, x, count() OVER (PARTITION BY k) AS c FROM (SELECT DISTINCT k, x FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 0, 1] AS x))
SETTINGS allow_distinct_partitions_independently = 1, query_plan_remove_redundant_distinct = 0;
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 1 FROM (EXPLAIN PIPELINE SELECT k, x, count() OVER (PARTITION BY k) AS c FROM (SELECT DISTINCT k, x FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 0, 1] AS x))
SETTINGS allow_distinct_partitions_independently = 1, query_plan_remove_redundant_distinct = 0;

-- Global size accounting supports string, LowCardinality, and nullable keys.
SELECT count(), sum(length(k)) FROM (SELECT DISTINCT toString(number % 1000) AS k FROM numbers_mt(50000))
SETTINGS max_rows_in_distinct = 2000, distinct_overflow_mode = 'break';
SELECT count(), sum(length(k)) FROM (SELECT DISTINCT toLowCardinality(toString(number % 1000)) AS k FROM numbers_mt(50000))
SETTINGS max_bytes_in_distinct = 10000000, distinct_overflow_mode = 'throw';
SELECT count(), countIf(k IS NULL) FROM (SELECT DISTINCT if(number % 1000 = 0, NULL, toString(number % 1000)) AS k FROM numbers_mt(50000))
SETTINGS max_rows_in_distinct = 2000, distinct_overflow_mode = 'break';

-- Disabling parallel `DISTINCT` retains the same aggregate results.
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(50000)) ARRAY JOIN [0, 1, 2] AS x GROUP BY k) SETTINGS allow_parallel_distinct = 0;
