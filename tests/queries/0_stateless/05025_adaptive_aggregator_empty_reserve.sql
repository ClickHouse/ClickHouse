-- Tags: long, no-flaky-check
-- no-flaky-check: the reserve-size check reads `trace_log`; concurrent copies overflow its lossy trace pipe and flush (as in `00974`).

-- Exercises the drain-time reserve of the string method's raw-string submap with short keys:
-- the keys never route to that submap, so its sampled share is zero and the drain reserves
-- zero additional entries for a submap that is also empty. A zero reservation must be a
-- no-op; a reservation that grows the table instead doubles the empty submap's buffer once
-- per pressure drain, and with a one-byte external threshold the sweeps run per block, so the
-- query's memory grows by powers of two into gigabytes. The memory limit is far above the
-- query's honest footprint and only the runaway growth can reach it.
SET max_memory_usage = 2000000000;
SET max_threads = 4;

SELECT
    (SELECT count(), sum(cityHash64(k)), sum(c) FROM (SELECT concat('key_', toString(number % 700)) AS k, count() AS c FROM numbers_mt(200000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0, enable_packed_string_keys_in_aggregation = 0))
    =
    (SELECT count(), sum(cityHash64(k)), sum(c) FROM (SELECT concat('key_', toString(number % 700)) AS k, count() AS c FROM numbers_mt(200000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, enable_packed_string_keys_in_aggregation = 0, adaptive_aggregator_freeze_threshold = 8, group_by_two_level_threshold = 1, max_block_size = 64, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0));

-- All-distinct keys make the drain's sampled insert rate exactly 1: each of the 256 buckets (~60k keys; ~30k for the
-- string method) must reserve a 2 MiB table, not 4 MiB (`< 64` tolerates one-off allocations elsewhere).
SET enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET max_block_size = 65536;
SET memory_profiler_sample_probability = 1, memory_profiler_sample_min_allocation_size = 1048576;

SELECT count(), sum(s) FROM (SELECT number AS k, sum(number) AS s FROM numbers_mt(15360000) GROUP BY k)
SETTINGS log_comment = 'number key';

SELECT count(), sum(s)
FROM (SELECT concat(toString(number), '-a-long-enough-key-suffix') AS k, sum(number) AS s FROM numbers_mt(7680000) GROUP BY k)
SETTINGS log_comment = 'string key', enable_packed_string_keys_in_aggregation = 0;

SYSTEM FLUSH LOGS query_log, trace_log;

SELECT q.log_comment, q.drained > 0 AS drain_ran, count() >= 64 AS tables_sampled, countIf(t.size >= 3145728) < 64 AS no_4_mib_tables
FROM system.trace_log AS t
INNER JOIN
(
    SELECT query_id, log_comment, ProfileEvents['AdaptiveAggregationDrainedRecords'] AS drained FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment LIKE '% key'
) AS q USING (query_id)
WHERE t.event_date >= yesterday() AND t.trace_type = 'MemorySample' AND t.size > 0
GROUP BY q.log_comment, q.drained
ORDER BY q.log_comment;
