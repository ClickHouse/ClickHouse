-- Isolate from the default ratio threshold: the spill must be triggered only by the explicit settings.
SET max_bytes_ratio_before_external_distinct = 0;

-- The final DISTINCT of a query with an ORDER BY expression runs above the sort, and its header
-- carries the sort column as a non-key, non-constant column. Such a column cannot be rebuilt from the
-- header, so the spill takes the buffered-chunks path and writes the column into the runs. The set of
-- distinct rows must match the in-memory result; the arrival order is not compared (after a spill the
-- merged runs return in DISTINCT-key order). The 1-byte threshold (with exact memory tracking and a
-- pinned block size) makes the spill deterministic.
SELECT count(), sum(cityHash64(k)) FROM (SELECT DISTINCT number % 300000 AS k FROM numbers_mt(3000000) ORDER BY k + 1 DESC) SETTINGS max_bytes_before_external_distinct = 0;
SELECT count(), sum(cityHash64(k)) FROM (SELECT DISTINCT number % 300000 AS k FROM numbers_mt(3000000) ORDER BY k + 1 DESC) SETTINGS max_bytes_before_external_distinct = 1, max_block_size = 65409, max_untracked_memory = 0, log_comment = '05049_external_distinct_non_key_columns/spill';

-- The spill did happen for the query above.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalDistinctWritePart'] > 0, ProfileEvents['ExternalDistinctMerge']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05049_external_distinct_non_key_columns/spill';
