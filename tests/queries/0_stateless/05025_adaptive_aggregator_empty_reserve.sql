-- Tags: long

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
