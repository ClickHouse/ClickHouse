SET max_threads = 1, optimize_distinct_in_order = 0, max_memory_usage_for_user = 536870912;

-- Zero thresholds disable external `DISTINCT`.
SELECT count() FROM
    (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers(10)
     SETTINGS max_bytes_before_external_distinct = 0, max_bytes_ratio_before_external_distinct = 0)
WHERE explain LIKE '%ExternalDistinctTransform%';

-- An absolute threshold enables external `DISTINCT` independently of the ratio.
SELECT count() FROM
    (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers(10)
     SETTINGS max_bytes_before_external_distinct = 1, max_bytes_ratio_before_external_distinct = 0)
WHERE explain LIKE '%ExternalDistinctTransform%';

-- A positive ratio enables external `DISTINCT` even when its byte threshold is less than one byte.
SELECT count() FROM
    (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers(10)
     SETTINGS max_bytes_before_external_distinct = 0, max_bytes_ratio_before_external_distinct = 1e-18)
WHERE explain LIKE '%ExternalDistinctTransform%';

-- Combining an absolute threshold with a ratio that produces less than one byte keeps spilling enabled.
SELECT count() FROM
    (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers(10)
     SETTINGS max_bytes_before_external_distinct = 1, max_bytes_ratio_before_external_distinct = 1e-18)
WHERE explain LIKE '%ExternalDistinctTransform%';
