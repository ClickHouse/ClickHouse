-- Disable external aggregation because the state is reset for each new block of data in 'runningAccumulate' function.
-- The randomized freeze thresholds of the adaptive aggregator change how its tables freeze and how it emits its
-- groups (blocks, order and the counters checked below), so pin their defaults.
SET adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = 4194304;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET allow_deprecated_error_prone_window_functions = 1;

SELECT k, finalizeAggregation(sum_state), runningAccumulate(sum_state) FROM (SELECT intDiv(number, 50000) AS k, sumState(number) AS sum_state FROM (SELECT number FROM system.numbers LIMIT 1000000) GROUP BY k ORDER BY k);
