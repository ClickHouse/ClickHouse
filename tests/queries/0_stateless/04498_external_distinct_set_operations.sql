-- The DISTINCT of UNION DISTINCT / INTERSECT DISTINCT / EXCEPT DISTINCT goes through the same
-- DistinctStep and must be able to spill.
SET max_bytes_ratio_before_external_distinct = 0;
SET max_bytes_before_external_distinct = 1;
SET max_untracked_memory = 0;
SET max_block_size = 1000;
SET log_comment = '04498_external_distinct_set_operations';

SELECT count() FROM (SELECT number % 1000 AS k FROM numbers(5000) UNION DISTINCT SELECT number % 1500 AS k FROM numbers(5000));
SELECT count() FROM (SELECT number % 1000 AS k FROM numbers(5000) INTERSECT DISTINCT SELECT number % 1500 AS k FROM numbers(5000));
SELECT count() FROM (SELECT number % 1000 AS k FROM numbers(5000) EXCEPT DISTINCT SELECT number % 500 AS k FROM numbers(1000));

SYSTEM FLUSH LOGS query_log;
SELECT countIf(ProfileEvents['ExternalDistinctWritePart'] >= 1) >= 3
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '04498_external_distinct_set_operations'
    AND query LIKE '%SELECT count%';
