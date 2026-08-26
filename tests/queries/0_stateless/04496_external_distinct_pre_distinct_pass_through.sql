SET max_bytes_ratio_before_external_distinct = 0;
-- Make sure the tiny threshold triggers deterministically (see 04495).
SET max_untracked_memory = 0;

-- Multi-stream query: a preliminary DISTINCT per stream plus the final DISTINCT. Under the tiny
-- threshold every preliminary DISTINCT immediately switches to pass-through, and the final DISTINCT
-- resolves all the duplicates by spilling. The result must still be exact.
SELECT count(), sum(cityHash64(k)) FROM (SELECT DISTINCT number % 100000 AS k FROM numbers_mt(1000000))
SETTINGS max_bytes_before_external_distinct = 1, max_threads = 4, log_comment = '04496_external_distinct/pass_through';

-- The plan indeed contains a preliminary DISTINCT (otherwise this test exercises nothing).
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT DISTINCT number % 100000 AS k FROM numbers_mt(1000000) SETTINGS max_threads = 4) WHERE explain LIKE '%Preliminary DISTINCT%';

-- The final DISTINCT did spill (proving the duplicates flowed through the pass-through and were resolved).
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalDistinctWritePart'] >= 1, ProfileEvents['ExternalDistinctMerge'] = 1
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '04496_external_distinct/pass_through';
