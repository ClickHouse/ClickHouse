-- Disable external aggregation because the state is reset for each new block of data in 'runningAccumulate' function.
SET max_bytes_before_external_group_by = 0;

SELECT k, finalizeAggregation(sum_state), runningAccumulate(sum_state) FROM (SELECT intDiv(number, 50000) AS k, sumState(number) AS sum_state FROM (SELECT number FROM system.numbers LIMIT 1000000) GROUP BY k ORDER BY k);
