-- Tags: no-asan, no-flaky-check
-- The max_memory_usage pair below depends on real memory accounting: the sanitizer overhead and the
-- flaky-check's concurrent reruns shift the memory usage enough to break the fails/succeeds contrast.

-- Isolate from the default ratio threshold: the spill must be triggered only by the explicit settings.
SET max_bytes_ratio_before_external_distinct = 0;

-- The result of DISTINCT with the spill must be identical to the in-memory result.
SELECT count(), sum(cityHash64(k)) FROM (SELECT DISTINCT number % 300000 AS k FROM numbers_mt(3000000)) SETTINGS max_bytes_before_external_distinct = 0;
SELECT count(), sum(cityHash64(k)) FROM (SELECT DISTINCT number % 300000 AS k FROM numbers_mt(3000000)) SETTINGS max_bytes_before_external_distinct = '4M';

-- Multiple spill rounds (every chunk is dumped as a separate run under the tiny threshold).
SELECT count(), sum(cityHash64(k)) FROM (SELECT DISTINCT number % 100000 AS k FROM numbers(300000)) SETTINGS max_bytes_before_external_distinct = 1, max_block_size = 65409, max_untracked_memory = 0;

-- The same query executed through the serialized query plan (the step settings round-trip).
SELECT count(), sum(cityHash64(k)) FROM (SELECT DISTINCT number % 100000 AS k FROM numbers(300000)) SETTINGS max_bytes_before_external_distinct = 1, max_block_size = 65409, max_untracked_memory = 0, serialize_query_plan = 1;

-- The spillable transform is used only when the threshold is set, and never for DISTINCT in order.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 2 AS k FROM numbers(1) SETTINGS max_bytes_before_external_distinct = 1) WHERE explain LIKE '%ExternalDistinctTransform%';
SELECT count() FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 2 AS k FROM numbers(1) SETTINGS max_bytes_before_external_distinct = 0) WHERE explain LIKE '%ExternalDistinctTransform%';
SELECT count() FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM (SELECT number AS k FROM numbers(10) ORDER BY k) SETTINGS max_bytes_before_external_distinct = 1, optimize_distinct_in_order = 1) WHERE explain LIKE '%ExternalDistinctTransform%';

-- A query that does not fit in memory fails without external DISTINCT and succeeds with it.
SELECT count() FROM (SELECT DISTINCT number % 8000000 AS k FROM numbers(16000000)) SETTINGS max_memory_usage = '120M', max_bytes_before_external_distinct = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT DISTINCT number % 8000000 AS k FROM numbers(16000000)) SETTINGS max_memory_usage = '120M', max_bytes_before_external_distinct = '30M';

-- No temporary files are written when the feature is disabled.
SELECT count() FROM (SELECT DISTINCT number % 1000 AS k FROM numbers(10000)) SETTINGS max_bytes_before_external_distinct = 0, log_comment = '04493_external_distinct/disabled';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalDistinctWritePart'], ProfileEvents['ExternalDistinctMerge']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '04493_external_distinct/disabled';
