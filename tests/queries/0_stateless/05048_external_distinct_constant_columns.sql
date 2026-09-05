-- Isolate from the default ratio threshold: the spill must be triggered only by the explicit settings.
SET max_bytes_ratio_before_external_distinct = 0;

-- Constant columns in the DISTINCT list are not written to the spilled runs and are re-attached to the
-- merged stream from the header, so the spilled result must match the in-memory result. The 1-byte
-- threshold (with exact memory tracking and a pinned block size) makes the spill deterministic. The
-- min/max pairs check the constant values on every row: the rows emitted before the spill carry the
-- original constants, while the rows returned after the merge carry the re-attached ones.
SELECT count(), sum(cityHash64(k)), min(c), max(c), min(s), max(s) FROM (SELECT DISTINCT 7 AS c, number % 300000 AS k, 'abc' AS s FROM numbers_mt(3000000)) SETTINGS max_bytes_before_external_distinct = 0;
SELECT count(), sum(cityHash64(k)), min(c), max(c), min(s), max(s) FROM (SELECT DISTINCT 7 AS c, number % 300000 AS k, 'abc' AS s FROM numbers_mt(3000000)) SETTINGS max_bytes_before_external_distinct = 1, max_block_size = 65409, max_untracked_memory = 0, log_comment = '05048_external_distinct_constant_columns/spill';

-- The spill did happen for the query above.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalDistinctWritePart'] > 0, ProfileEvents['ExternalDistinctMerge']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05048_external_distinct_constant_columns/spill';
